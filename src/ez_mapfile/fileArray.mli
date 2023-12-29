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

include FILE

type index = int (* position of value in array *)
type pos = int   (*  position of value in file *)

(* ?zero: a value that is so common that we don't store it in the
   array, but use internally the 0-position to encode that it is
   there.  It's a simple way to compress the table dynamically. Only
   useful with variable-size encodings.

   ?index_enc: the encoding of the integer used for the position of
   the value in the FileSequence in the case of values with
   variable-size encoding. By default, it is a 64-bit Network Order,
   but shorter encodings can be used for smaller tables.

   ?async: whether the size, length (number of items) and no-usage of
   the file are updated immediatly or only on commit & close
   operations. true by default. Note that this parameter has little
   impact in the case of memory-mapped files, that are only sync on
   commit & close anyway.

 *)

(* ?magic="EZMAPARR" *)
val create :
  ?magic:string ->
  ?config:FileEngine.config ->
  ?index_enc: index FileEncoding.t ->
  ?counters: int ->
  ?async: bool ->
  mode ->
  'value FileEncoding.t ->
  ?zero:'value ->
  string ->
  'value t

(* Warning: `set` does not allow holes in the array. It must be used either
   to set an existing position, or at the end to add a new position *)
val set : 'value t -> index -> 'value -> unit
val get : 'value t -> index -> 'value

val add : 'value t -> 'value -> int
(* Add n times element to the array. *)
val init : 'value t -> int -> 'value -> unit

val iter : ('value -> unit) -> 'value t -> unit
val iteri : (int -> 'value -> unit) -> 'value t -> unit

(*
Only useful for variable-size encodings. Remove duplicates from `file_seq`.
*)

val compress : 'value t -> unit
