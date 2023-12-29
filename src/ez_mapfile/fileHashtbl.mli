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

type ('key,'value) t

val create :
  ?magic:string ->
  ?config:FileEngine.config ->
  ?table_enc: int FileEncoding.t ->
  ?async: bool ->
  mode ->
  key_enc:'key FileEncoding.t ->
  value_enc:'value FileEncoding.t ->
  ?initial_size:int ->
  string -> ('key, 'value) t

(* `add t key value`: add a new entry binding `key` to `value`. Any
   former binding still exists, but is hidden by this new
   binding. *)
val add: ('key,'value) t -> 'key -> 'value -> unit

(* `find t key`: return the value associated in the latest binding
   with `key`, or raise the `Not_found` exception if no such binding
   exists. *)
val find: ('key,'value) t -> 'key -> 'value

(* `find_opt t key`: same as `find t key`, but returns an option to
   avoid raising an exception. *)
val find_opt: ('key,'value) t -> 'key -> 'value option

(* `find_all t key`: returns the list of all values bound to this
   key. The first value in the list is the most recent one, with
   other binding coming in the reverse-order of addition. *)
val find_all: ('key,'value) t -> 'key -> 'value list

(* `iter f t`: apply function `f` on all bindings in the table. For
   a given key, iter on all bindings in the reverse-order of
   addition (i.e. the latest comes first).  *)
val iter : ('key -> 'value -> unit) -> ('key, 'value) t -> unit

(* `remove ?all t key`: remove the latest binding associated with
   `key` in the table. If `all` is set to `true`, remove all the
   bindings associated with `key`, not only the latest. *)
val remove : ?all:bool -> ('key, 'value) t -> 'key -> unit

(* From FILE. Same documentation as above *)

val commit : ?sync:bool -> ('key, 'value) t -> unit
val close : ?again:bool -> ('key,'value) t -> unit
val closed : ('key,'value) t -> bool

val rename : ('key,'value) t -> string -> unit
val delete : ('key,'value) t -> unit

val config : ('key,'value) t -> FileEngine.config
val filename : ('key,'value) t -> string
val filenames : ('key,'value) t -> string list

(* `length t`: returns the current number of bindings in the table. *)
val length : ('key,'value) t -> int


type stats = {
  entries : FileSequence.stats ;
  collisions : FileSequence.stats ;
  table_size : int ;
}

(* TODO : make new_size optional. Compute missing clean and new_size
   depending on stats *)
val resize : ?clean:bool ->  ?table_enc: int FileEncoding.t -> ?new_size:int ->
  ('key,'value) t -> unit
val stats : ('key,'value) t -> stats
