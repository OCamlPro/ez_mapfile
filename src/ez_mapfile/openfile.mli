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

type t

(* ?readonly: default is false
   ?create: default is `not ~readonly` (to reuse a file in read-write mode,
    you should use ~create:false.
 *)

val openfile : ?readonly:bool -> ?create:bool -> string -> t

(* write a bytes array at a given position, and return the next position *)
val write_string : t -> off:int -> string -> pos:int -> len:int -> int

(* write a bytes array at a given position, without header *)
val write_fixed_string : t -> off:int -> string -> pos:int -> len:int -> unit

val read_string : t -> off:int -> string

val read_fixed_bytes : t -> off:int -> bytes -> pos:int -> len:int -> unit

val read_fixed_string : t -> off:int -> len:int -> string

val after_string : t -> off:int -> int

val close : t -> unit

val write_int : t -> off:int -> FileEncoding.int_format -> int -> int

val read_int : t -> off:int -> FileEncoding.int_format -> int
