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

(* TODO: we can probably reduce the number of `lseek` by keeping up-to-date
   our current position in the file. *)

type t = {
  fd : Unix.file_descr ;
  mutable pos : int ;
}

let openfile ?(readonly=false) ?(create=not readonly) file_name =
  let flags = match readonly, create with
      true, _ ->      [ Unix.O_RDONLY ]
    | false, true ->  [ Unix.O_RDWR ; Unix.O_CREAT ]
    | false, false -> [ Unix.O_RDWR ]
  in
  let fd = Unix.openfile file_name flags 0o644 in
  let pos = 0 in
  { fd ; pos }

let goto_pos t ~off =
  if t.pos <> off then begin
    let off2 = Unix.lseek t.fd off Unix.SEEK_SET in
    assert ( off = off2 );
    t.pos <- off
  end


let write_fixed_string t ~off s ~pos ~len =
  goto_pos t ~off;
  let n = Unix.write_substring t.fd s pos len in
  assert ( n = len );
  t.pos <- t.pos + len

let write_fixed_bytes t ~off b ~pos ~len =
  goto_pos t ~off;
  let n = Unix.write t.fd b pos len in
  assert ( n = len );
  t.pos <- t.pos + len

let rec really_read t b pos len =
  if len > 0 then
    let n = Unix.read t.fd b pos len in
    if n = 0 then failwith "Unix.read failed";
    t.pos <- t.pos + n ;
    really_read t b pos (len-n)

let read_fixed_bytes t ~off b ~pos ~len =
  (* Printf.eprintf "read_fixed_bytes ~off:%d ~len:%d\n%!" off len; *)
  goto_pos t ~off;
  really_read t b pos len

let read_fixed_string t ~off ~len =
  let b = Bytes.create len in
  read_fixed_bytes t ~off b ~pos:0 ~len;
  Bytes.unsafe_to_string b

let close t = Unix.close t.fd

let write_int t ~off int_format x =
  let b = FileEncoding.bytes_of_int int_format x in
  let len = Bytes.length b in
  write_fixed_bytes t ~off b ~pos:0 ~len;
  off + len

let read_int t ~off int_format =
  let len = FileEncoding.size_of_int_format int_format in
  let s = read_fixed_string t ~off ~len in
  FileEncoding.int_of_string int_format s


let write_string t ~off s ~pos ~len =
  let b = Bytes.create 8 in
  let rec iter pos len =
    if len < 128 then begin
      Bytes.set b pos @@ char_of_int @@ len lor 128;
      pos+1
    end else begin
      Bytes.set b pos @@ char_of_int @@ len land 0x7f;
      iter (pos+1) (len lsr 7)
    end
  in
  let int_len = iter 0 len in
  write_fixed_bytes t ~off b ~pos:0 ~len:int_len;
  let n = Unix.write_substring t.fd s pos len in
  t.pos <- t.pos + n;
  assert ( n = len );
  let size = int_len + len in
  let size = if size < 8 then begin
      let padding = 8 - size in
      let n = Unix.write_substring t.fd "\000\000\000\000\000\000\000\000" 0 padding in
      t.pos <- t.pos + n;
      assert ( n = padding );
      8
    end else size in
  off + size

let read_variable_int t ~off =
  let s = read_fixed_string t ~off ~len:8 in
  let rec iter pos v bits =
    let c = int_of_char s.[pos] in
    let pos = pos + 1 in
    let d = c land 0x7f in
    let v = v + ( d lsl bits ) in
    let bits = bits + 7 in
    assert (bits < 56);
    if c = d then
      iter pos v bits
    else
      v, off+pos
  in
  iter 0 0 0

let read_string t ~off =
  (* Printf.eprintf "read_string ~off:%d\n%!" off; *)
  let len, off = read_variable_int t ~off in
  read_fixed_string t ~off ~len


let after_string t ~off =
  let len, off = read_variable_int t ~off in
  off + len
