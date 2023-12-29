(**************************************************************************)
(*                                                                        *)
(*  Copyright (c) 2023 OCamlPro SAS                                       *)
(*                                                                        *)
(*  All rights reserved.                                                  *)
(*  This file is distributed under the terms of the GNU Lesser General    *)
(*  Public License version 2.1, with the special exception on linking     *)
(*  described in the LICENSE.md file in the root directory.               *)
(*                                                                        *)
(*                                                                        *)
(**************************************************************************)

(* This version uses only 3 files *)

type collision_pos = int
type entry_pos = int

(* keys are limited to 65535 bytes when encoded, and max 2 billion entries *)

type ('a,'b) t = {
  mutable file_entries : ('a * string) FileSequence.t ;
  value_enc : 'b FileEncoding.t ;
  magic : string;
  initial_size : int ;
  mutable table_enc : int FileEncoding.t ;

  (* hash -> position in file_collisions *)
  mutable file_table : collision_pos FileArray.t ;

  (* since the encoding is constant-size, we could use a FileArray instead *)
  mutable file_collisions : (entry_pos * collision_pos) FileSequence.t ;
}

let filename_of_entries file_name = file_name ^ ".entries"
let filename_of_collisions file_name = file_name ^ ".collisions"

let collision_enc =
  FileEncoding.create_string
    ~size:(Some 12)
    ~magic:""
    ~encode:(fun (entry_pos,next_pos) ->
        let b = Bytes.create 12 in
        EndianBytes.LittleEndian.set_int64 b 0 @@ Int64.of_int entry_pos;
        EndianBytes.LittleEndian.set_int32 b 8 @@ Int32.of_int next_pos;
        Bytes.unsafe_to_string b
      )
    ~decode:(fun s ->
        let entry_pos = EndianString.LittleEndian.get_int64 s 0 |> Int64.to_int in
        let next_pos = EndianString.LittleEndian.get_int32 s 8 |> Int32.to_int in
        ( entry_pos, next_pos )
      )

let entry_enc ?value_size key_enc =
  let key_size = FileEncoding.size key_enc in
  let size = match value_size, key_size with
      Some value_size, Some key_size -> Some ( value_size + key_size )
    | _ -> None
  in
  FileEncoding.create_string
    ~size
    ~magic:"KEYVAL"
    ~encode:(fun (key,value_string) ->
        let key_string = FileEncoding.encode key_enc key in
        let key_len = String.length key_string in
        let value_len = String.length value_string in
        let b, pos =
          match key_size with
          | None ->
              let b = Bytes.create (2 + key_len + value_len) in
              EndianBytes.LittleEndian.set_int16 b 0 key_len ;
              b, 2
          | Some key_size ->
              assert ( key_size = key_len );
              let b = Bytes.create ( key_len + value_len ) in
              b, 0
        in
        Bytes.blit_string key_string   0 b pos key_len ;
        Bytes.blit_string value_string 0 b (pos+key_len) value_len ;
        Bytes.unsafe_to_string b
      )
    ~decode:(fun s ->
        let len = String.length s in
        let key_len, pos = match key_size with
          | None -> EndianString.LittleEndian.get_int16 s 0, 2
          | Some key_size -> key_size, 0
        in
        let key_string = String.sub s pos key_len in
        let value_len = len - pos - key_len in
        let value_string = String.sub s (pos+key_len) value_len in
        let key = FileEncoding.decode key_enc key_string in
        ( key, value_string )
      )

let create ?(magic="EZMAPH3F") ?config ?(table_enc=FileEncoding.int32no "")
    ?async mode ~key_enc ~value_enc
    ?(initial_size=8191) file_name
  =

  let entry_enc = entry_enc key_enc in
  let file_entries =
    FileSequence.create ~magic:(magic ^ "ENT") ?config ?async mode
      entry_enc
      (filename_of_entries file_name)
  in

  let file_table =
    FileArray.create ~magic:(magic ^ "TAB") ?config ?async mode
      table_enc file_name
  in

  let file_collisions =
    FileSequence.create ~magic:(magic ^ "COL") ?config ?async
      mode collision_enc
      (filename_of_collisions file_name)
  in

  begin
    match mode with
    | ReadOnly -> ()
    | ReadWrite -> ()
    | Create ->
        FileArray.init file_table initial_size (-1);
        (*
        Printf.eprintf "test next_pos\n%!";
        let v = FileArray.get file_table 0 in
        assert (v = -1)
*)
  end;
  {
    table_enc ;
    file_entries ;
    magic;
    value_enc ;
    file_table ;
    file_collisions ;
    initial_size ;
  }

let close ?again t =
  FileSequence.close ?again t.file_entries;
  FileArray.close ?again t.file_table;
  FileSequence.close ?again t.file_collisions;
  ()

let closed t = FileSequence.closed t.file_entries

type stats = {
  entries : FileSequence.stats ;
  collisions : FileSequence.stats ;
  table_size : int ;
}

let stats t =
  let entries = FileSequence.stats t.file_entries in
  let collisions = FileSequence.stats t.file_collisions in
  let table_size = FileArray.length t.file_table in
  { entries ; collisions ; table_size }

let resize ?clean ?table_enc ?new_size t =
  (*
Printf.eprintf "******************* resize %d *********************\n%!"
    new_size;
*)
  let file_name = FileArray.filename t.file_table in
  let config = FileArray.config t.file_table in
  let new_table_file = file_name ^ ".new" in
  let new_collisions_file = filename_of_collisions file_name ^ ".new" in
  let new_entries_file = filename_of_entries file_name ^ ".new" in

  let table_enc = match table_enc with
    | None -> t.table_enc
    | Some table_enc -> table_enc
  in

  let new_size = match new_size, clean with
    | Some new_size, None -> Some (new_size, false)
    | Some new_size, Some clean -> Some (new_size, clean)
    | None, clean ->
        let clean = match clean with
          | None -> false
          | Some clean -> clean
        in
        let s = stats t in
        let table_len = s.table_size in
        if table_len > s.entries.length * 8 then
          Some ( max (s.entries.length * 2 + 1) t.initial_size, clean )
        else
        if clean then
          Some (table_len, true)
        else
          None
  in

  match new_size with
  | None -> () (* nothing to do *)
  | Some (new_size, clean) ->

      let async = FileSequence.async t.file_entries in
      let new_table =
        FileArray.create ~magic:(t.magic ^ "TAB") ~config ~async Create
          table_enc new_table_file
      in
      FileArray.init new_table new_size (-1);

      let new_collisions =
        FileSequence.create ~magic:(t.magic ^ "COL") ~config ~async Create
          collision_enc
          new_collisions_file
      in

      let new_entries = if clean then
          let file =
            FileSequence.create ~magic:(t.magic ^ "ENT") ~config ~async Create
              (FileSequence.encoding t.file_entries)
              new_entries_file
          in
          Some file
        else None
      in

      let rec iter old_col_pos =
        if old_col_pos <> -1 then begin

          let entry_pos, next_old_pos =
            FileSequence.get t.file_collisions old_col_pos in

          (* We must insert in the same order, so start with the end of
             the collision list *)
          iter next_old_pos ;

          let key, value_string = FileSequence.get t.file_entries entry_pos in
          let entry_pos = match new_entries with
            | None -> entry_pos
            | Some file_entries ->
                FileSequence.add file_entries ( key, value_string )
          in

          let hash = Hashtbl.hash key in
          let table_pos = hash mod new_size in
          let next_pos = FileArray.get new_table table_pos in

          let col_pos = FileSequence.add new_collisions ( entry_pos, next_pos ) in
          FileArray.set new_table table_pos col_pos
        end
      in

      for table_pos = 0 to FileArray.length t.file_table - 1 do
        let col_pos = FileArray.get t.file_table table_pos in
        iter col_pos
      done;

      let old_table_file = file_name ^ ".old" in
      let old_collisions_file = filename_of_collisions file_name ^ ".old" in
      let old_entries_file = filename_of_entries file_name ^ ".old" in

      let old_table = t.file_table in
      let old_collisions = t.file_collisions in
      let old_entries = t.file_entries in

      t.table_enc <- table_enc ;
      t.file_table <- new_table ;
      t.file_collisions <- new_collisions ;

      begin match new_entries with
        | None -> ()
        | Some file_entries ->
            t.file_entries <- file_entries ;
      end;

      FileArray.rename old_table old_table_file ;
      FileSequence.rename old_collisions old_collisions_file ;
      if clean then
        FileSequence.rename old_entries old_entries_file ;

      FileArray.rename t.file_table file_name ;
      FileSequence.rename t.file_collisions (filename_of_collisions file_name);
      if clean then begin
        FileSequence.rename t.file_entries (filename_of_entries file_name);
        FileSequence.delete old_entries
      end;

      FileArray.delete old_table;
      FileSequence.delete old_collisions;

      ()

let iter f t =

  let rec iter col_pos =
    if col_pos <> -1 then

      let entry_pos, next_pos =
        FileSequence.get t.file_collisions col_pos in

      let key, value_string = FileSequence.get t.file_entries entry_pos in
      let value = FileEncoding.decode t.value_enc value_string in

      f key value ;

      iter next_pos ;

  in

  for table_pos = 0 to FileArray.length t.file_table - 1 do
    let col_pos = FileArray.get t.file_table table_pos in
    iter col_pos
  done

let remove ?(all=false) t key =
  let hash = Hashtbl.hash key in
  let table_len = FileArray.length t.file_table in
  let table_pos = hash mod table_len in
  let col_pos = FileArray.get t.file_table table_pos in

  let rec iter col_pos =
    if col_pos = -1 then
      col_pos
    else
      let entry_pos, next_pos = FileSequence.get t.file_collisions col_pos in
      let key2, _ = FileSequence.get t.file_entries entry_pos in
      if key = key2 then begin
        FileSequence.add_no_usage t.file_entries entry_pos ;
        FileSequence.add_no_usage t.file_collisions col_pos ;
        if all then
          iter next_pos
        else
          next_pos
      end else
        let new_next_pos = iter next_pos in
        if new_next_pos <> next_pos then
          FileSequence.set t.file_collisions col_pos
            (entry_pos, new_next_pos);
        col_pos
  in
  let new_col_pos = iter col_pos in
  if new_col_pos <> col_pos then
    FileArray.set t.file_table table_pos new_col_pos

let add t key value =
  let value_string = FileEncoding.encode t.value_enc value in
  let entry_pos = FileSequence.add t.file_entries ( key, value_string ) in

  let len = FileSequence.length t.file_entries in
  let table_len = FileArray.length t.file_table in
  let table_len =
    if 3 * len > table_len then
      let new_size = 2 * table_len - 1 in
      resize t ~new_size ;
      new_size
    else
      table_len
  in

  (* Printf.eprintf "add.table_len = %d\n%!" table_len;*)
  let hash = Hashtbl.hash key in
  let table_pos = hash mod table_len in
  (* Printf.eprintf "add.table_pos = %d\n%!" table_pos; *)
  let next_pos = FileArray.get t.file_table table_pos in
  (* Printf.eprintf "add.next_pos = %d\n%!" next_pos; *)

  let col_pos = FileSequence.add t.file_collisions ( entry_pos, next_pos ) in
  (* Printf.eprintf "add.col_pos = %d\n%!" col_pos; *)
  FileArray.set t.file_table table_pos col_pos;

  (*
  let entry_pos2, next_pos2 = FileSequence.get t.file_collisions col_pos in
  assert (entry_pos2 = entry_pos);
  assert (next_pos = next_pos2);
*)
  ()

let find_opt t key =
  let hash = Hashtbl.hash key in
  let table_len = FileArray.length t.file_table in
  (* Printf.eprintf "table_len = %d\n%!" table_len; *)
  let table_pos = hash mod table_len in
  (* Printf.eprintf "table_pos = %d\n%!" table_pos; *)
  let col_pos = FileArray.get t.file_table table_pos in
  let rec iter pos =
    (* Printf.eprintf "   iter pos = %d\n%!" pos; *)
    if pos = -1 then None
    else
      let () = () in
      (* Printf.eprintf "  get_col...\n%!"; *)
      let entry_pos, next_pos = FileSequence.get t.file_collisions pos in
      (*  Printf.eprintf "  entry_pos=%d next_pos=%d\n%!" entry_pos next_pos; *)
      let key2, value_string = FileSequence.get t.file_entries entry_pos in
      (* Printf.eprintf "  key read\n%!" ; *)
      if key = key2 then begin
        (* Printf.eprintf "  good key\n%!"; *)
        Some (FileEncoding.decode t.value_enc value_string)
      end else
        iter next_pos
  in
  iter col_pos

let find t key =
  match find_opt t key with
  | None -> raise Not_found
  | Some value -> value

let find_all t key =
  let hash = Hashtbl.hash key in
  let table_len = FileArray.length t.file_table in
  let table_pos = hash mod table_len in
  let col_pos = FileArray.get t.file_table table_pos in
  let rec iter pos res =
    if pos = -1 then
      List.rev res
    else
      let entry_pos, next_pos = FileSequence.get t.file_collisions pos in
      let key2, value_string = FileSequence.get t.file_entries entry_pos in
      let res =
        if key = key2 then
          (FileEncoding.decode t.value_enc value_string) :: res
        else
          res
      in
      iter next_pos res
  in
  iter col_pos []

let length t = FileSequence.length t.file_entries

let config t = FileArray.config t.file_table
let filename t = FileArray.filename t.file_table
let filenames t =
  List.flatten [
    FileSequence.filenames t.file_entries ;
    FileArray.filenames t.file_table ;
    FileSequence.filenames t.file_collisions ;
  ]
let delete t =
  close ~again:true t;
  List.iter Sys.remove (filenames t)

let rename t file_name =
  FileArray.rename t.file_table file_name ;
  FileSequence.rename t.file_entries (filename_of_entries file_name) ;
  FileSequence.rename t.file_collisions (filename_of_collisions file_name )

let commit ?sync t =
  FileArray.commit ?sync t.file_table ;
  FileSequence.commit ?sync t.file_entries ;
  FileSequence.commit ?sync t.file_collisions
