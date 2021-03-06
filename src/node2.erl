-module(node2).
-author("Achil").

-define(Stabilize, 100).
-define(Timeout, 5000).

%% API
-export([start/1, start/2]).

start(Id) ->
  start(Id, nil).

start(Id, Peer) ->
  timer:start(),
  spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
  Predecessor = nil,
  {ok, Successor} = connect(Id, Peer),
  schedule_stabilize(),
  node(Id, Predecessor, Successor, storage:create()).

connect(Id, nil) ->
  {ok, {Id, self()}};
connect(Id, Peer) ->
  Qref = make_ref(),
  Peer ! {key, Qref, self()},
  receive
    {Qref, Skey} ->
      Peer ! {notify, {Id, self()}},
      {ok, {Skey, Peer}}
  after ?Timeout ->
    io:format("Time out: no response~n", [])
  end.

schedule_stabilize() ->
  timer:send_interval(?Stabilize, self(), stabilize).


stabilize({_, Spid}) ->
  Spid ! {request, self()}.

node(Id, Predecessor, Successor, Store) ->
  receive
  % key request
    {key, Qref, Peer} ->
      Peer ! {Qref, Id},
      node(Id, Predecessor, Successor, Store);
  % new node
    {notify, New} ->
      {Pred, NewStore} = notify(New, Id, Predecessor, Store),
      node(Id, Pred, Successor, NewStore);
  % a predecessor needs to know our predecessor
    {request, Peer} ->
      request(Peer, Predecessor),
      node(Id, Predecessor, Successor, Store);
  % our successor informs us about its predecessor
    {status, Pred} ->
      Succ = stabilize(Pred, Id, Successor),
      node(Id, Predecessor, Succ, Store);
  % stabilize request
    stabilize ->
      stabilize(Successor),
      node(Id, Predecessor, Successor, Store);
    probe ->
      create_probe(Id, Successor),
      node(Id, Predecessor, Successor, Store);
    {probe, Id, Nodes, T} ->
      remove_probe(T, Nodes),
      node(Id, Predecessor, Successor, Store);
    {probe, Ref, Nodes, T} ->
      forward_probe(Ref, T, Nodes, Id, Successor),
      node(Id, Predecessor, Successor, Store);
    state ->
      io:format("~w ~w ~w ~p~n",[Id, Predecessor, Successor, Store]),
      node(Id, Predecessor, Successor, Store);
    {state, Peer} ->
      Peer ! {Id, Predecessor, Successor, Store},
      node(Id, Predecessor, Successor, Store);
    exit ->
      exit(requested);

    {add, Key, Value, Qref, Client} ->
      Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Added);
    {lookup, Key, Qref, Client} ->
      lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Store);
    {handover, Elements} ->
      Merged = storage:merge(Store, Elements),
      node(Id, Predecessor, Successor, Merged);

    Nonsense -> io:format("~p received weird message: ~p", [Id, Nonsense])
  end.

handover(Id, Store, Nkey, Npid) ->
  {Keep, Rest} = storage:split(Nkey, Id, Store),
  Npid ! {handover, Rest},
  Keep.

lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, Store) ->
  case key:between(Key, Pkey, Id) of
    true ->
      Result = storage:lookup(Key, Store),
      Client ! {Qref, Result};
    false ->
      {_, Spid} = Successor,
      Spid ! {lookup, Key, Qref, Client}
  end.

add(Key, Value, Qref, Client, Id, {Pkey, _}, {_, Spid}, Store) ->
  case key:between(Key, Pkey, Id) of
    true ->
      Client ! {Qref, ok},
      storage:add(Key, Value, Store);
    false ->
      Spid ! {add, Key, Value, Qref, Client},
      Store
  end.

forward_probe(Ref, T, Nodes, Id, {_Sid, Spid}) ->
  Spid ! {probe, Ref, [Id | Nodes], T}.

remove_probe(T, Nodes) ->
  Elapsed = erlang:system_time(micro_seconds) - T,
  io:format("probe: {time: ~w, nodes: ~p}", [Elapsed, Nodes]).

create_probe(Id, {_Sid, Spid}) ->
  Spid ! {probe, Id, [Id], erlang:system_time(micro_seconds)}.

notify({Nkey, Npid}, Id, Predecessor, Store) ->
  case Predecessor of
    nil ->
      Keep = handover(Id, Store, Nkey, Npid),
      {{Nkey, Npid}, Keep};
    {Pkey, _} ->
      case key:between(Nkey, Pkey, Id) of
        true ->
          Keep = handover(Id, Store, Nkey, Npid),
          {{Nkey, Npid}, Keep};
        false ->
          {Predecessor, Store}
      end
  end.

request(Peer, Predecessor) ->
  Peer ! {status, Predecessor}.

stabilize(Pred, Id, Successor) ->
  {Skey, Spid} = Successor,
  case Pred of
    nil ->
      Spid ! {notify, {Id, self()}},
      Successor;
    {Id, _} ->
      Successor;
    {Skey, _} ->
      Spid ! {notify, {Id, self()}},
      Successor;
    {Xkey, Xpid} ->
      case key:between(Xkey, Id, Skey) of
        true ->
          Xpid ! {request, self()},
          Pred;
        false ->
          Spid ! {notify, {Id, self()}},
          Successor
      end
  end.




















