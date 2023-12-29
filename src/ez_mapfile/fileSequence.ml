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

type pos = int

(* TODO: currently, writes are particularly slow on openfile, because
   we keep the `end_pos` and `length` counters always synchronized in
   the file. We could add a flag `?sync=false` that would only set
   these counters on commit & close operations.
*)

type 'a t = {
  mutable file_name : string ;
  mutable file : FileEngine.t ;
  async : bool ;
  mutable end_pos : int ;
  mutable nitems : int ;
  mutable no_usage : int ;
  mutable modified : bool ;

  mode : Types.mode ;
  end_pos_pos : int ;
  encoding : 'a FileEncoding.t ;
  mutable closed : bool ;
  zero : 'a option ;
  magic : string ;
  counters : int ;
}

let encoding t = t.encoding
let config t = FileEngine.config t.file
let filename t = t.file_name
let filenames t = [t.file_name]
let async t = t.async

let get_end_pos t =
  FileEngine.read_int t.file ~off:t.end_pos_pos Int63NO

let set_end_pos t end_pos =
  let (_ : int) =
    FileEngine.write_int t.file ~off:t.end_pos_pos Int63NO end_pos
  in
  (*
  if get_end_pos t <> end_pos then begin
    Printf.eprintf "file end_pos %d <> written end_pos %d\n%!" ( get_end_pos t ) end_pos;
    exit 2
  end;
*)
  ()

let get_nitems t =
  let nitems = FileEngine.read_int t.file ~off:(t.end_pos_pos + 8) Int63NO in
  (*  Printf.eprintf "get_nitems %d\n%!" nitems; *)
  nitems

let set_nitems t nitems =
  (*  Printf.eprintf "set_nitems %d\n%!" nitems; *)
  let (_ : int) =
    FileEngine.write_int t.file ~off:(t.end_pos_pos + 8) Int63NO nitems
  in
  ()

let get_no_usage t =
  FileEngine.read_int t.file ~off:(t.end_pos_pos + 16) Int63NO

let set_no_usage t no_usage =
  let (_ : int) =
    FileEngine.write_int t.file ~off:(t.end_pos_pos + 16) Int63NO no_usage
  in
  ()

let counters_pos t = t.end_pos_pos + 24
let counters t = t.counters
let get_counter t c =
  FileEngine.read_int t.file ~off:(counters_pos t + c * 8) Int63NO
let set_counter t c n =
  let ( _ : int ) = FileEngine.write_int t.file ~off:(counters_pos t + c * 8) Int63NO n in
  ()

let first_pos t = counters_pos t + t.counters * 8

let not_closed t op =
  if t.closed then
    error t.file_name (OperationOnClosedFile op)

let create ?(magic="EZMAPSEQ") ?config ?(counters = 0) ?(async=true)
    mode encoding ?zero file_name =
  let create,readonly = match mode with
    | Create -> true, false
    | ReadOnly -> false, true
    | ReadWrite -> false, false
  in
  let file = FileEngine.openfile ?config ~readonly ~create file_name in
  let enc_magic = FileEncoding.magic encoding in
  let long_magic = magic ^ enc_magic in
  let magic_len = String.length long_magic in
  let end_pos_pos = magic_len in
  begin
    match mode with
    | Create ->
        FileEngine.write_fixed_string file ~off:0 long_magic ~pos:0 ~len:magic_len
    | _ ->
        let file_magic_bytes = Bytes.create magic_len in
        FileEngine.read_fixed_bytes file ~off:0
          file_magic_bytes ~pos:0 ~len:magic_len ;

        let file_magic = Bytes.to_string file_magic_bytes in
        if file_magic <> long_magic then begin
          FileEngine.close file;
          error file_name (BadMagic (file_magic, long_magic));
        end
  end;
  let t =
    {
      file_name ;
      file ;
      mode ;
      end_pos_pos ;
      encoding ;
      closed = false ;
      zero ;
      magic ;
      counters ;
      end_pos = 0 ;
      nitems = 0 ;
      no_usage = 0 ;
      async ;
      modified = false ;
    }
  in
  begin
    match mode with
    | Create ->
        let first_pos = first_pos t in
        set_end_pos t first_pos ;
        t.end_pos <- first_pos ;
        set_nitems t 0;
        set_no_usage t 0;
    | _ ->
        if async then begin
          t.nitems <- get_nitems t ;
          t.end_pos <- get_end_pos t ;
          t.no_usage <- get_no_usage t ;
        end
  end;
  t

let length t =
  if t.async then
    t.nitems
  else
    get_nitems t

let end_pos t =
  if t.async then
    t.end_pos
  else
    get_end_pos t

let get (type s) (t : s t) pos =
  not_closed t "get";
  match pos, t.zero with
  | 0, Some zero -> zero
  | _ ->
      let end_pos = end_pos t in
      if pos < t.end_pos_pos + 8 || pos >= end_pos then
        error t.file_name (OutOfBounds ("FileSequence.get", pos));

        match t.encoding with
        | IntEnc custom ->
            let s = FileEngine.read_int t.file ~off:pos custom.size in
            FileEncoding.custom_decode custom s
        | StringEnc custom ->
            let s = match custom.size with
              | None ->
                  FileEngine.read_string t.file ~off:pos
              | Some len ->
                  FileEngine.read_fixed_string t.file ~off:pos ~len
            in
            FileEncoding.custom_decode custom s

let add (type s) (t : s t) ( x : s) =
  not_closed t "add";
  begin
    match t.mode with
    | ReadOnly -> error t.file_name WriteToReadOnlyFile
    | _ -> ()
  end;
  match t.zero with
  | Some zero when zero = x -> 0
  | _ ->
      let cur_pos = end_pos t in
      assert ( cur_pos <> 0);
      let end_pos =
        match t.encoding with
        | IntEnc custom ->
            let s = FileEncoding.custom_encode custom x in
            FileEngine.write_int t.file ~off:cur_pos custom.size s
        | StringEnc custom ->
            let s = FileEncoding.custom_encode custom x in
            let len = String.length s in
            match custom.size with
            | None ->
                (*                Printf.eprintf "FileSequence.add with var-len encoding off=%d len=%d\n%!" cur_pos len; *)
                let end_pos = FileEngine.write_string t.file ~off:cur_pos
                    s ~pos:0 ~len in
                (*
                let s' = FileEngine.read_string t.file ~off:cur_pos in
                if s <> s' then begin
                  Printf.eprintf " s  = %S\n%!" s;
                  Printf.eprintf " s' = %S\n%!" s';
                  exit 2
                end; *)
                end_pos

            | Some size ->
                assert ( len = size );
                FileEngine.write_fixed_string t.file ~off:cur_pos
                  s ~pos:0 ~len;
                cur_pos + len
      in
      if t.async then begin
        t.end_pos <- end_pos ;
        t.nitems <- t.nitems + 1;
        t.modified <- true
      end else begin
        set_end_pos t end_pos;
        set_nitems t ( get_nitems t + 1 );
      end;
      cur_pos

let init (type s) (t : s t) n ( x : s) =
  not_closed t "init";
  begin
    match t.mode with
    | ReadOnly -> error t.file_name WriteToReadOnlyFile
    | _ ->
        match t.zero with
        | Some _ -> error t.file_name ( NotWorkingWithZero "init" )
        | _ -> ()
  end;
  if n > 0 then
    let cur_pos = end_pos t in
    let end_pos =
      match t.encoding with
      | IntEnc custom ->
          let s = FileEncoding.custom_encode custom x in
          let rec iter n pos =
            if n > 0 then
              let pos = FileEngine.write_int t.file ~off:pos custom.size s in
              iter (n-1) pos
            else
              pos
          in
          iter n cur_pos
      | StringEnc custom ->
          let s = FileEncoding.custom_encode custom x in
          let len = String.length s in
          match custom.size with
          | None ->
              error t.file_name (IncompatibleVariableSizeEncoding "init")
          | Some size ->
              assert ( len = size );
              let rec iter n pos =
                if n > 0 then
                  let pos =
                    FileEngine.write_fixed_string t.file ~off:pos
                      s ~pos:0 ~len;
                    pos + len
                  in
                  iter (n-1) pos
                else
                  pos
              in
              iter n cur_pos
    in
    if t.async then begin
      t.end_pos <- end_pos ;
      t.nitems <- t.nitems + n;
      t.modified <- true
    end else begin
      set_end_pos t end_pos;
      set_nitems t ( get_nitems t + n );
    end ;
    ()

let next_pos (type s) ( t : s t) pos =
  not_closed t "next_pos";
  match t.encoding with
  | IntEnc custom ->
      let size = FileEncoding.size_of_int_format custom.size in
      pos + size
  | StringEnc custom ->
      match custom.size with
      | None ->
          FileEngine.after_string t.file ~off:pos
      | Some size ->
          pos + size

let end_pos t =
  not_closed t "end_pos";
  end_pos t

(* TODO: check that pos < end_pos *)
let set (type s) (t : s t) pos ( x : s) =
  (*  Printf.eprintf "FileSequence.set at pos %d\n%!" pos; *)
  not_closed t "set";
  begin
    match t.mode with
    | ReadOnly -> error t.file_name WriteToReadOnlyFile
    | _ -> ()
  end;
  begin
    match t.encoding with
    | IntEnc custom ->
        let s = FileEncoding.custom_encode custom x in
        let ( _end_pos : int ) = FileEngine.write_int t.file ~off:pos custom.size s in
        ()
    | StringEnc custom ->
        let s = FileEncoding.custom_encode custom x in
        let len = String.length s in
        match custom.size with
        | None -> error t.file_name (IncompatibleVariableSizeEncoding "set")
        | Some size ->
            assert ( len = size );
            FileEngine.write_fixed_string t.file ~off:pos
              s ~pos:0 ~len
  end

let closed t = t.closed

let close ?(again=false) t =
  if closed t then begin
    if not again then not_closed t "close";
  end else begin
    if t.async && t.modified then begin
      set_end_pos t t.end_pos ;
      set_nitems t t.nitems ;
      set_no_usage t t.no_usage ;
      t.modified <- false;
    end;
    FileEngine.close t.file;
    t.closed <- true
  end

type 'a iterator = {
  iterator_file : 'a t ;
  mutable iterator_pos : pos ;
  iterator_end_pos : pos option ;
}

let iterator_create ?begin_pos ?end_pos t =
  begin
    match t.zero with
    | Some _ -> error t.file_name ( NotWorkingWithZero "iterator_create" )
    | _ -> ()
  end;
  let begin_pos = match begin_pos with
    | None -> first_pos t
    | Some first_pos -> first_pos
  in
  {
    iterator_file = t ;
    iterator_pos = begin_pos ;
    iterator_end_pos = end_pos ;
  }

let iterator_get iter =
  let t = iter.iterator_file in
  let end_pos = match iter.iterator_end_pos with
    | Some end_pos -> end_pos
    | None -> end_pos t
  in
  let pos = iter.iterator_pos in
  if pos >= end_pos then None
  else
    let v = get t pos in
    let pos = next_pos t pos in
    iter.iterator_pos <- pos;
    Some v

let iter ?begin_pos ?end_pos f t =
  let i = iterator_create ?begin_pos ?end_pos t in
  let rec iter f i =
    match iterator_get i with
    | None -> ()
    | Some x ->
        f x ;
        iter f i
  in
  iter f i

let length t =
  length t

let add_no_usage t cur_pos =
  if cur_pos <> 0 then
    let end_pos = next_pos t cur_pos in
    if t.async then begin
      t.no_usage <- t.no_usage + ( end_pos - cur_pos ) ;
      t.nitems <- t.nitems - 1 ;
      t.modified <- true ;
    end else begin
      set_no_usage t ( get_no_usage t + ( end_pos - cur_pos ) );
      set_nitems t ( length t - 1 )
    end

let rename t file_name =
  let was_closed = closed t in
  if not was_closed then close t;
  Sys.rename t.file_name file_name;
  if not was_closed then
    let create,readonly = match t.mode with
      | Create -> true, false
      | ReadOnly -> false, true
      | ReadWrite -> false, false
    in
    let config = FileEngine.config t.file in
    let file = FileEngine.openfile ~config ~readonly ~create file_name in
    t.file <- file ;
    t.file_name <- file_name ;
    t.closed <- false

let delete t =
  close ~again:true t;
  List.iter Sys.remove (filenames t)

type stats = {
  end_pos : int ;
  length : int ;
  no_usage : int ;
}

let no_usage t =
  if t.async then
    t.no_usage
  else
    get_no_usage t

let stats t =
  let end_pos = end_pos t in
  let length = length t in
  let no_usage = no_usage t in
  { end_pos ; length ; no_usage }

let zero t = t.zero
let magic t = t.magic

let commit ?(sync=true) t =
  if t.async && t.modified then begin
    set_end_pos t t.end_pos ;
    set_nitems t t.nitems ;
    set_no_usage t t.no_usage ;
    t.modified <- false;
  end;
  FileEngine.commit t.file ~sync
