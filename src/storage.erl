-module(storage).
-author("Achil").

%% API
-export([create/0,add/3,lookup/2,split/3,merge/2]).

create() -> [].

add(Key, Value, Store) ->
  [{Key, Value} | Store].

lookup(Key, Store) ->
  lists:keyfind(Key, 1, Store).

split(From, To, Store) ->
  splitter(From, To, Store, [], []).

splitter(_From, _To, [], Updated, Rest) -> {Updated, Rest};
splitter(From, To, [{Key, Value} | Store], Updated, Rest) ->
  case key:between(Key, From, To) of
    true -> splitter(From, To, Store, [{Key, Value} | Updated], Rest);
    false -> splitter(From, To, Store, Updated, [{Key, Value} | Rest])
  end.

merge(Entries, Store) ->
  lists:append(Entries, Store).

