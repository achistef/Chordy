-module(node4).
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
  Next = nil,
  {ok, Successor} = connect(Id, Peer),
  schedule_stabilize(),
  node(Id, Predecessor, Successor, storage:create(), Next).

connect(Id, nil) ->
  Ref = monitor(self()),
  {ok, {Id, Ref, self()}};
connect(Id, Peer) ->
  Qref = make_ref(),
  Peer ! {key, Qref, self()},
  receive
    {Qref, Skey} ->
      Peer ! {notify, {Id, self()}},
      Ref = monitor(Peer),
      {ok, {Skey, Ref, Peer}}
  after ?Timeout ->
    io:format("Time out: no response~n", [])
  end.

schedule_stabilize() ->
  timer:send_interval(?Stabilize, self(), stabilize).


stabilize(nil) -> ok;
stabilize({_, _, Spid}) ->
  Spid ! {request, self()}.

node(Id, Predecessor, Successor, Store, Next) ->
  receive
  % key request
    {key, Qref, Peer} ->
      Peer ! {Qref, Id},
      node(Id, Predecessor, Successor, Store, Next);
  % new node
    {notify, New} ->
      {Pred, NewStore} = notify(New, Id, Predecessor, Store),
      node(Id, Pred, Successor, NewStore, Next);
  % a predecessor needs to know our predecessor & successor
    {request, Peer} ->
      request(Peer, Predecessor, Successor),
      node(Id, Predecessor, Successor, Store, Next);
  % our successor informs us about its predecessor and successor
    {status, Pred, Nx} ->
      {Succ, Nxt} = stabilize(Pred, Id, Successor, Nx),
      node(Id, Predecessor, Succ, Store, Nxt);
  % stabilize request
    stabilize ->
      stabilize(Successor),
      node(Id, Predecessor, Successor, Store, Next);
    probe ->
      create_probe(Id, Successor),
      node(Id, Predecessor, Successor, Store, Next);
    {probe, Id, Nodes, T} ->
      remove_probe(T, Nodes),
      node(Id, Predecessor, Successor, Store, Next);
    {probe, Ref, Nodes, T} ->
      forward_probe(Ref, T, Nodes, Id, Successor),
      node(Id, Predecessor, Successor, Store, Next);
    state ->
      io:format("~w ~w ~w ~w ~p~n", [Id, Predecessor, Successor, Next, Store]),
      node(Id, Predecessor, Successor, Store, Next);
    {state, Peer} ->
      Peer ! {Id, Predecessor, Successor, Next, Store},
      node(Id, Predecessor, Successor, Store, Next);
    exit ->
      exit(requested);
    {'DOWN', Ref, process, _, _} ->
      {Pred, Succ, Nxt} = down(Ref, Predecessor, Successor, Next),
      node(Id, Pred, Succ, Store, Nxt);

    {add, Key, Value, Qref, Client} ->
      Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Added, Next);
    {lookup, Key, Qref, Client} ->
      lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Store, Next);
    {handover, Elements} ->
      Merged = storage:merge(Store, Elements),
      node(Id, Predecessor, Successor, Merged, Next);

    Nonsense ->
      io:format("~p received weird message: ~p", [Id, Nonsense]),
      node(Id, Predecessor, Successor, Store, Next)
  end.

monitor(Pid) -> erlang:monitor(process, Pid).
drop(nil) -> ok;
drop(Pid) -> erlang:demonitor(Pid, [flush]).

handover(Id, Store, Nkey, Npid) ->
  {Keep, Rest} = storage:split(Nkey, Id, Store),
  Npid ! {handover, Rest},
  Keep.

lookup(Key, Qref, Client, Id, {Pkey, _, _}, Successor, Store) ->
  case key:between(Key, Pkey, Id) of
    true ->
      Result = storage:lookup(Key, Store),
      Client ! {Qref, Result};
    false ->
      {_, _, Spid} = Successor,
      Spid ! {lookup, Key, Qref, Client}
  end.

add(Key, Value, Qref, Client, Id, {Pkey, _, _}, {_, _, Spid}, Store) ->
  case key:between(Key, Pkey, Id) of
    true ->
      Client ! {Qref, ok},
      storage:add(Key, Value, Store);
    false ->
      Spid ! {add, Key, Value, Qref, Client},
      Store
  end.

forward_probe(Ref, T, Nodes, Id, {_Sid, _Sref, Spid}) ->
  Spid ! {probe, Ref, [Id | Nodes], T}.

remove_probe(T, Nodes) ->
  Elapsed = erlang:system_time(micro_seconds) - T,
  io:format("probe: {time: ~w, nodes: ~p}", [Elapsed, Nodes]).

create_probe(Id, {_Sid, _Sref, Spid}) ->
  Spid ! {probe, Id, [Id], erlang:system_time(micro_seconds)}.

notify({Nkey, Npid}, Id, Predecessor, Store) ->
  case Predecessor of
    nil ->
      Keep = handover(Id, Store, Nkey, Npid),
      Nref = monitor(Npid),
      {{Nkey, Nref, Npid}, Keep};
    {Pkey, Pref, _} ->
      case key:between(Nkey, Pkey, Id) of
        true ->
          Keep = handover(Id, Store, Nkey, Npid),
          drop(Pref),
          Nref = monitor(Npid),
          {{Nkey, Nref, Npid}, Keep};
        false ->
          {Predecessor, Store}
      end
  end.

request(Peer, Predecessor, Successor) ->
  Pr = case Predecessor of
         nil -> nil;
         {Pkey, _, Ppid} -> {Pkey, Ppid}
       end,
  Sc = case Successor of
         nil -> nil;
         {Skey, _, Spid} -> {Skey, Spid}
       end,
  Peer ! {status, Pr, Sc}.

stabilize(Pred, Id, Successor, Next) ->
  {Skey, Sref, Spid} = Successor,
  case Pred of
    nil ->
      Spid ! {notify, {Id, self()}},
      {Successor, Next};
    {Id, _} ->
      {Successor, Next};
    {Skey, _} ->
      Spid ! {notify, {Id, self()}},
      {Successor, Next};
    {Xkey, Xpid} ->
      case key:between(Xkey, Id, Skey) of
        true ->
          drop(Sref),
          Xref = monitor(Xpid),
          Xpid ! {request, self()},
          {{Xkey, Xref, Xpid}, Successor};
        false ->
          Spid ! {notify, {Id, self()}},
          {Successor, Next}
      end
  end.

down(Ref, {_, Ref, _}, Successor, Next) -> {nil, Successor, Next};
down(Ref, Predecessor, {_, Ref, _}, {Nkey, Npid}) ->
  Nref = monitor(Npid),
  {Predecessor, {Nkey, Nref, Npid}, nil}.




















