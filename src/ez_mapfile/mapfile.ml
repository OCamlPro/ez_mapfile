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

type t (* a mapped file *)

external openfile :
  filename: string ->
  readonly: bool ->
  mapsize: int64 ->
  create: bool ->
  t
  = "ocp_mapfile_openfile_c"

let openfile ?mapsize ?readonly ?create filename =
  let readonly = match readonly with
    | Some readonly -> readonly | None -> false
  in
  let create = match create with
    | Some create -> create | None -> not readonly
  in
  let mapsize = match mapsize with
    | Some mapsize -> mapsize | None -> 1_000_000_000L
  in
  openfile ~mapsize ~readonly ~create ~filename

(* write a bytes array at a given position, and return the next position *)
external write_string :
  t -> off:int -> string -> pos:int -> len:int -> int
  = "ocp_mapfile_write_string_c"

(* write a bytes array at a given position, without header, and return
   the next position *)
external write_fixed_string :
  t -> off:int -> string -> pos:int -> len:int -> unit
  = "ocp_mapfile_write_fixed_string_c"

(* write a char array at a given position, and return the next position *)
external write_bytes : t -> off:int -> bytes -> pos:int -> len:int -> int
  = "ocp_mapfile_write_string_c"

(* write a char array at a given position, without header, and return
   the next position *)
external write_fixed_bytes : t -> off:int -> bytes -> pos:int -> len:int -> unit
  = "ocp_mapfile_write_fixed_string_c"

(* write a char array at a given position, and return the next position *)
external write :
  t -> off:int -> Bigstring.t -> pos:int -> len:int -> int
  = "ocp_mapfile_write_bigstring_c"

(* write a char array at a given position, without header, and return
   the next position *)
external write_fixed :
  t -> off:int -> Bigstring.t -> pos:int -> len:int -> unit
  = "ocp_mapfile_write_fixed_bigstring_c"

(* read from a given position, returning a Bigstring.t containing the
   content (that was written with `write_bytes`) *)
external read : t -> off:int -> Bigstring.t
  = "ocp_mapfile_read_bigstring_c"

external read_fixed : t -> off:int -> len:int -> Bigstring.t
  = "ocp_mapfile_read_fixed_bigstring_c"

external read_bytes : t -> off:int -> bytes -> pos:int -> int
  = "ocp_mapfile_read_bytes_c"

external read_string : t -> off:int -> string
  = "ocp_mapfile_read_string_c"

external read_fixed_bytes : t -> off:int -> bytes -> pos:int -> len:int -> unit
  = "ocp_mapfile_read_fixed_bytes_c"

external read_fixed_string : t -> off:int -> len:int -> string
  = "ocp_mapfile_read_fixed_string_c"

external read_size : t -> off:int -> int
  = "ocp_mapfile_read_int_variable_c"

external after_string : t -> off:int -> int
  = "ocp_mapfile_after_string_c"

external commit : t -> sync:bool -> unit
  = "ocp_mapfile_commit_c"

external close : t -> unit
  = "ocp_mapfile_close_c"

(* the returned size might be smaller than the file for read-only access *)
external size : t -> int
  = "ocp_mapfile_size_c"

external write_int_variable : t -> off:int -> int -> int
  = "ocp_mapfile_write_int_variable_c"

external read_int_variable : t -> off:int -> int
  = "ocp_mapfile_read_int_variable_c"

let int_size n =
  let rec iter n size =
    let n = n lsr 7 in
    if n = 0 then size else iter n (1+size)
  in
  iter n 1

(* Truncate to a smaller size *)
external truncate : t -> int -> unit
  = "ocp_mapfile_truncate_c"

(* Warning: you must never use write_fixed, wirte_int or
   write_ocaml_string on a file if you plan to use this function. *)
let iter t ?(off=0) ~max_off f =
  let rec iter t ~off ~max_off f =
    if off < max_off then
      let s = read t ~off in
      f ~off s;
      let off = after_string t ~off in
      iter t ~off ~max_off f
  in
  iter t ~off ~max_off f


(* reading and writing fixed integers *)

external write_int : t -> off:int -> FileEncoding.int_format -> int -> int =
  "ocp_mapfile_write_int_c"

external read_int : t -> off:int -> FileEncoding.int_format -> int =
  "ocp_mapfile_read_int_c"

(* Warning: a mapfile should never be unmapped after read_ocaml_string,
   because the GC might want to access the storage to read the header
   of the string (because with 5.+, there are no-naked-pointers. *)

external read_ocaml_string : t -> off:int -> string
  = "ocp_mapfile_read_ocaml_string_c"

external write_ocaml_string : t -> off:int -> string -> int
  = "ocp_mapfile_write_ocaml_string_c"
