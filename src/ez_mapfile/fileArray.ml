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

open Types

type indexer =
  | EncodingSize of int
  | IndexFile of int FileSequence.t

type 'a t = {
  mutable file_name : string ;
  mutable file_seq : 'a FileSequence.t ;
  file_idx : indexer ;
  magic : string ;
  zero : 'a option;
  index_size : int ;
}

type index = int (* position of value in array *)
type pos = int   (*  position of value in file *)

let filename_of_index file_name = file_name ^ ".index"

let create ?(magic="EZMAPARR") ?config
    ?(index_enc = FileEncoding.int63no "" ) ?(counters=0)
    ?async
    mode encoding ?zero file_name =

  let index_size = match FileEncoding.size index_enc with
    | None -> error file_name
                (IncompatibleVariableSizeEncoding "FileArray.create ~index_enc")
    | Some size -> size
  in
  let file_idx =
    match FileEncoding.size encoding with
    | Some size -> EncodingSize size
    | None ->
        let magic = magic ^ "IDX" in
        let file_idx = FileSequence.create ~magic ?config ?async mode
            index_enc
            (filename_of_index file_name) in
        IndexFile file_idx
  in

  let zero = match zero, file_idx with
    (* ~zero does not make sense with fixed-size encoding *)
    | Some _zero, EncodingSize _ -> None
    | zero, _ -> zero
  in

  let file_seq =
    FileSequence.create ~magic ~counters ?config mode encoding ?zero
      ?async file_name
  in

  {
    file_name ;
    file_seq ;
    file_idx ;
    magic ;
    zero ;
    index_size ;
  }

let config t = FileSequence.config t.file_seq
let filename t = FileSequence.filename t.file_seq
let filenames t =
  FileSequence.filenames t.file_seq @
  (match t.file_idx with
   | EncodingSize _ -> []
   | IndexFile file_idx -> FileSequence.filenames file_idx)

let get_pos t pos = FileSequence.get t.file_seq pos
let set_pos t pos v = FileSequence.set t.file_seq pos v

let length t =
  match t.file_idx with
  | EncodingSize _ ->
      FileSequence.length t.file_seq
  | IndexFile file_idx ->
      FileSequence.length file_idx

let file_idx_pos t file_idx index =
  FileSequence.first_pos file_idx + index * t.index_size

let get t index =
  let len = length t in
  if index >= len then
    error t.file_name (OutOfBounds ("FileArray.get", index));
  let pos =
    match t.file_idx with
    | EncodingSize size ->
        FileSequence.first_pos t.file_seq + index * size
    | IndexFile file_idx ->
        let idx_pos = file_idx_pos t file_idx index in
        let pos = FileSequence.get file_idx idx_pos in
        pos
  in
  get_pos t pos

let add t v =
  let index = length t in
  let pos = FileSequence.add t.file_seq v in
  (* Printf.eprintf "FileArray.add length=%d pos=%d\n%!" index pos; *)
  begin
    match t.file_idx with
    | EncodingSize size ->
        assert ( pos =
                 FileSequence.first_pos t.file_seq + index * size )
    | IndexFile file_idx ->
        let idx_pos = FileSequence.add file_idx pos in
        let expected_pos = file_idx_pos t file_idx index in
        if idx_pos <> expected_pos then begin
          Printf.eprintf "FileArray.add wrong returned pos:\n%!";
          Printf.eprintf "  idx_pos      = %d\n%!" idx_pos ;
          Printf.eprintf "  expected_pos = %d\n%!" expected_pos ;
          exit 2
        end

  end;
  index

let set t index v =
  let len = length t in
  if index > len then
    error t.file_name (OutOfBounds ("FileArray.set", index));
  if index = len then
    let ( _ : int ) = add t v in
    ()
  else
    match t.file_idx with
    | EncodingSize size ->
        let pos = FileSequence.first_pos t.file_seq + index * size in
        set_pos t pos v
    | IndexFile file_idx ->
        let idx_pos = file_idx_pos t file_idx index in
        let new_pos = FileSequence.add t.file_seq v in
        FileSequence.set file_idx idx_pos new_pos;
        ()

let init t n v =
  match t.file_idx with
  | EncodingSize _ ->
      FileSequence.init t.file_seq n v
  | IndexFile file_idx ->
      let pos = FileSequence.add t.file_seq v in
      FileSequence.init file_idx n pos

let close ?again t =
  FileSequence.close ?again t.file_seq;
  match t.file_idx with
  | EncodingSize _ -> ()
  | IndexFile file_idx ->
      FileSequence.close ?again file_idx
let closed t = FileSequence.closed t.file_seq

let commit ?sync t =
  FileSequence.commit ?sync t.file_seq ;
  begin
    match t.file_idx with
    | EncodingSize _ -> ()
    | IndexFile file_idx ->
        FileSequence.commit ?sync file_idx
  end;
  ()

let rename t file_name =
  FileSequence.rename t.file_seq file_name ;
  begin
    match t.file_idx with
    | EncodingSize _ -> ()
    | IndexFile file_idx ->
        FileSequence.rename file_idx ( filename_of_index file_name )
  end;
  t.file_name <- file_name

let iter f t =
  let len = length t in
  for i = 0 to len - 1 do
    f (get t i)
  done

let iteri f t =
  let len = length t in
  for i = 0 to len - 1 do
    f i (get t i)
  done

let delete t =
  close ~again:true t;
  List.iter Sys.remove (filenames t)

let compress t =
  match t.file_idx with
  | EncodingSize _ -> () (* nothing to do for fixed-size encodings *)
  | IndexFile file_idx ->

      let file_name = FileSequence.filename t.file_seq in
      let magic = FileSequence.magic t.file_seq in
      let zero = FileSequence.zero t.file_seq in
      let config = FileSequence.config t.file_seq in
      let encoding = FileSequence.encoding t.file_seq in
      let counters = FileSequence.counters t.file_seq in
      let async = FileSequence.async t.file_seq in

      let new_file_name = file_name ^ ".new" in
      let new_file_seq =
        FileSequence.create ~magic ~config ~counters
          ~async Create encoding ?zero new_file_name
      in

      let len = FileSequence.length file_idx in

      let h = Hashtbl.create (2*len+1) in
      for i = 0 to len - 1 do

        (* Printf.eprintf "index: %d\n%!" i; *)
        let idx_pos = file_idx_pos t file_idx i in
        let pos = FileSequence.get file_idx idx_pos in
        let v = FileSequence.get t.file_seq pos in
        let pos =
          match Hashtbl.find h v with
          | exception Not_found ->
              let pos = FileSequence.add new_file_seq v in
              (* Printf.eprintf "new_pos = %d\n%!" pos; *)
              Hashtbl.add h v pos;
              pos
          | pos ->
              pos
        in
        FileSequence.set file_idx idx_pos pos
      done;

      let old_file_seq = t.file_seq in
      let old_file_name = file_name ^ ".old" in

      t.file_seq <- new_file_seq ;
      FileSequence.rename old_file_seq old_file_name ;
      FileSequence.rename t.file_seq file_name ;
      FileSequence.delete old_file_seq;
      (* Printf.eprintf "compress done\n%!"; *)
      ()

let counters t = FileSequence.counters t.file_seq
let get_counter t c = FileSequence.get_counter t.file_seq c
let set_counter t c n = FileSequence.set_counter t.file_seq c n

let async t = FileSequence.async t.file_seq
