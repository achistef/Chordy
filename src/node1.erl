-module(node1).
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
  node(Id, Predecessor, Successor).

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
    io:format("Time out: no response~n",[])
  end.

schedule_stabilize() ->
  timer:send_interval(?Stabilize, self(), stabilize).


stabilize({_, Spid}) ->
  Spid ! {request, self()}.

node(Id, Predecessor, Successor) ->
  receive
  % key request
    {key, Qref, Peer} ->
      Peer ! {Qref, Id},
      node(Id, Predecessor, Successor);
  % new node
    {notify, New} ->
      Pred = notify(New, Id, Predecessor),
      node(Id, Pred, Successor);
  % a predecessor needs to know our predecessor
    {request, Peer} ->
      request(Peer, Predecessor),
      node(Id, Predecessor, Successor);
  % our successor informs us about its predecessor
    {status, Pred} ->
      Succ = stabilize(Pred, Id, Successor),
      node(Id, Predecessor, Succ);
  % stabilize request
    stabilize ->
      stabilize(Successor),
      node(Id, Predecessor, Successor);
    probe ->
      create_probe(Id, Successor),
      node(Id, Predecessor, Successor);
    {probe, Id, Nodes, T} ->
      remove_probe(T, Nodes),
      node(Id, Predecessor, Successor);
    {probe, Ref, Nodes, T} ->
      forward_probe(Ref, T, Nodes, Id, Successor),
      node(Id, Predecessor, Successor);
    {state, Peer} ->
      Peer ! {Id, Predecessor, Successor},
      node(Id, Predecessor, Successor);
    exit ->
      exit(requested);
    Nonsense -> io:format("~p received weird message: ~p", [Id, Nonsense])
  end.

forward_probe(Ref, T, Nodes, Id, {_Sid, Spid})->
  Spid ! {probe, Ref, [Id|Nodes], T}.

remove_probe(T, Nodes) ->
  Elapsed = erlang:system_time(micro_seconds)-T,
  io:format("probe: {time: ~w, nodes: ~p}",[Elapsed,Nodes]).

create_probe(Id, {_Sid, Spid}) ->
  Spid ! {probe, Id, [Id], erlang:system_time(micro_seconds)}.


notify({Nkey, Npid}, Id, Predecessor) ->
  case Predecessor of
    nil ->
      {Nkey, Npid};
    {Pkey, _} ->
      case key:between(Nkey, Pkey, Id) of
        true ->
          {Nkey, Npid};
        false ->
          Npid ! {status, Predecessor},
          Predecessor
      end
  end.

request(Peer, Predecessor) ->
  case Predecessor of
    nil ->
      Peer ! {status, nil};
    {Pkey, Ppid} ->
      Peer ! {status, {Pkey, Ppid}}
  end.

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
