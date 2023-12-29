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

module TYPES = struct

  type 't tmv = {
    write_string : 't -> off:int -> string -> pos:int -> len:int -> int;
    write_fixed_string : 't -> off:int -> string -> pos:int -> len:int -> unit;
    read_string : 't -> off:int -> string;
    read_fixed_bytes : 't -> off:int -> bytes -> pos:int -> len:int -> unit;
    read_fixed_string : 't -> off:int -> len:int -> string;
    after_string : 't -> off:int -> int;
    close : 't -> unit;
    write_int : 't -> off:int -> FileEncoding.int_format -> int -> int;
    read_int : 't -> off:int -> FileEncoding.int_format -> int;
    commit : 't -> sync:bool -> unit
  }


  type file =
    | OtherFile : 'a * 'a tmv -> file

end

open TYPES

type config = (?readonly:bool -> ?create:bool -> string -> file)

let new_config f = f

let openfile_tmv = {
  write_string = Openfile.write_string ;
  write_fixed_string = Openfile.write_fixed_string ;
  read_string = Openfile.read_string ;
  read_fixed_bytes = Openfile.read_fixed_bytes ;
  read_fixed_string = Openfile.read_fixed_string ;
  after_string = Openfile.after_string ;
  close = Openfile.close ;
  write_int = Openfile.write_int ;
  read_int = Openfile.read_int ;
  commit = (fun _ ~sync:_ -> ());
}

let openfile_config ?readonly ?create file_name =
  let file = Openfile.openfile ?readonly ?create file_name in
  OtherFile (file, openfile_tmv)


let tmv = {
  write_string = Mapfile.write_string ;
  write_fixed_string = Mapfile.write_fixed_string ;
  read_string = Mapfile.read_string ;
  read_fixed_bytes = Mapfile.read_fixed_bytes ;
  read_fixed_string = Mapfile.read_fixed_string ;
  after_string = Mapfile.after_string ;
  close = Mapfile.close ;
  write_int = Mapfile.write_int ;
  read_int = Mapfile.read_int ;
  commit = Mapfile.commit ;
}

let mapfile_config ~mapsize =
  new_config
    (fun ?readonly ?create file_name ->
       let file = Mapfile.openfile ?mapsize ?readonly ?create file_name in
       OtherFile (file, tmv))

let default_config_ref = ref (mapfile_config ~mapsize:None)

let set_default_config config = default_config_ref := config
let default_config () = !default_config_ref

type t = {
  config : config ;
  file : file ;
}

let config t = t.config

let openfile ?( config = default_config ()) ?readonly ?create file_name =
  let file = config ?readonly ?create file_name in
  {
    config ; file
  }

let write_string t ~off s ~pos ~len =
  match t.file with
  | OtherFile (t, tmv) ->
      tmv.write_string t ~off s ~pos ~len

let write_fixed_string t ~off s ~pos ~len =
  match t.file with
  | OtherFile (t, tmv) ->
      tmv.write_fixed_string t ~off s ~pos ~len

let read_string t ~off =
  match t.file with
  | OtherFile (t, tmv) ->
      tmv.read_string t ~off

let read_fixed_bytes t ~off b ~pos ~len =
  match t.file with
  | OtherFile (t, tmv) ->
      tmv. read_fixed_bytes t ~off b ~pos ~len

let read_fixed_string t ~off ~len =
  match t.file with
  | OtherFile (t, tmv) ->
      tmv.read_fixed_string t ~off ~len

let after_string t ~off =
  match t.file with
  | OtherFile (t, tmv) ->
      tmv.after_string t ~off

let close t=
  match t.file with
  | OtherFile (t, tmv) ->
      tmv.close t

let write_int t ~off int_format x =
  match t.file with
  | OtherFile (t, tmv) ->
      tmv.write_int t ~off int_format x

let read_int t ~off int_format =
  match t.file with
  | OtherFile (t, tmv) ->
      tmv.read_int t ~off int_format

let commit t ~sync =
  match t.file with
  | OtherFile (t, tmv) ->
      tmv.commit t ~sync
