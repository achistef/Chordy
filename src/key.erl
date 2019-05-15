-module(key).
-author("Achil").

%% API
-export([generate/0, between/3]).

generate() -> random:uniform(1000000).

between(_Key, From, From) -> true;
between(Key, From, To) when To < From -> not between(Key, To, From);
between(Key, From, To) -> (Key > From) and (Key =< To).

