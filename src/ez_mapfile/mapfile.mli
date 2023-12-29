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

(* We ensure that write_bytes/write_string ALWAYS introduces at least
   a gap of 8 bytes. The cost on Tezos storage is less than 1%.

   There are two kinds of accesses:
   * `write_string/write/write_bytes` and equivalent `read_string/read`:
   these accesses rely on a format of the file where the current position
   stores the length of the following content, and then the content itself.
   The length is encoded in a variable-length format, with 7-bits per byte
   until the last byte with a higher 1-bit set. The content itself is not
   0-terminated. If the size of the length and the size of the content is
   smaller than 8 bytes, it is padded until 8 bytes. For read operations,
   Bigstrings point to the content in the file (and thus, should not be edited),
   while strings are copies.

   Because the format contains the size of the content, it is possible to
   use the `iter` function to iterate on all strings.

   * `write_fixed*`/`read_fixed*`/`write_int`/`read_int`: these functions
   all work directly on the content, without writing/reading any header.
 *)

type t (* a mapped file *)

(* ?mapsize: default is 1 GB
   ?readonly: default is false
   ?create: default is `not ~readonly`
   File is opened with MAP_SHARED.
 *)
val openfile :
  ?mapsize:int64 -> ?readonly:bool -> ?create:bool -> string -> t

(* write a bytes array at a given position, and return the next position *)
external write_string :
  t -> off:int -> string -> pos:int -> len:int -> int
  = "ocp_mapfile_write_string_c"

(* write a bytes array at a given position, without header *)
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

(* write a char array at a given position, without header *)
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

(* Warning: you must never use write_fixed or write_ocaml_string on a
   file if you plan to use this function. *)
val iter :
  t -> ?off:int -> max_off:int -> (off:int -> Bigstring.t -> unit) -> unit

(* Truncate to a smaller size *)
external truncate : t -> int -> unit
  = "ocp_mapfile_truncate_c"

val int_size : int -> int (* size in 7-bit representation *)

(* reading and writing fixed integers *)

external write_int : t -> off:int -> FileEncoding.int_format -> int -> int =
  "ocp_mapfile_write_int_c"

external read_int : t -> off:int -> FileEncoding.int_format -> int =
  "ocp_mapfile_read_int_c"

(* a mapfile should never be unmapped after read_ocaml_string, because the
   GC might want to access the storage to read the header of the string
   (because with 5.+, there are no-naked-pointers. *)

external read_ocaml_string : t -> off:int -> string
  = "ocp_mapfile_read_ocaml_string_c"

external write_ocaml_string : t -> off:int -> string -> int
  = "ocp_mapfile_write_ocaml_string_c"
