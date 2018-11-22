%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2018 Pivotal Software, Inc.
%%
%% @doc
%% This module offers a framework to declare capabilities a RabbitMQ node
%% supports and therefore a way to determine if multiple RabbitMQ nodes in
%% a cluster are compatible and can work together.
%%
%% == What a feature flag is ==
%%
%% A <strong>feature flag</strong> is a name and several properties given
%% to a change in RabbitMQ which impacts its communication with other
%% RabbitMQ nodes. This kind of change can be:
%% <ul>
%% <li>an update to an Erlang record</li>
%% <li>a modification to a replicated Mnesia table schema</li>
%% <li>a modification to Erlang messages exchanged between Erlang processes
%%   which might run on remote nodes</li>
%% </ul>
%%
%% A feature flag is qualified by:
%% <ul>
%% <li>a <strong>name</strong></li>
%% <li>a <strong>description</strong> (optional)</li>
%% <li>a list of other <strong>feature flags this feature flag depends on
%%   </strong> (optional). This can be useful when the change builds up on
%%   top of a previous change. For instance, it expands a record which was
%%   already modified by a previous feature flag.</li>
%% <li>a <strong>migration function</strong> (optional). If provided, this
%%   function is called when the feature flag is enabled. It is responsible
%%   for doing all the data conversion, if any, and confirming the feature
%%   flag can be enabled.</li>
%% <li>a level of stability (stable or experimental). For now, this is only
%%   informational. But it might be used for specific purposes in the
%%   future.</li>
%% </ul>
%%
%% == How to declare a feature flag ==
%%
%% To define a new feature flag, you need to use the `rabbitmq_feature_flag()'
%% module attribute:
%%
%% ```
%% -rabitmq_feature_flag(FeatureFlag).
%% '''
%%
%% `FeatureFlag' is a {@type feature_flag_modattr()}.

-module(rabbit_feature_flags).

-export([list/0,
         list/1,
         list/2,
         enable/1,
         enable_all/0,
         disable/1,
         disable_all/0,
         is_supported/1,
         is_supported_locally/1,
         is_supported_remotely/1,
         is_supported_remotely/2,
         are_supported/1,
         are_supported_locally/1,
         are_supported_remotely/1,
         are_supported_remotely/2,
         is_enabled/1,
         is_enabled/2,
         is_disabled/1,
         is_disabled/2,
         info/0,
         info/1,
         init/0,
         get_state/1,
         get_stability/1,
         check_node_compatibility/1,
         check_node_compatibility/2,
         is_node_compatible/1,
         is_node_compatible/2,
         sync_feature_flags_with_cluster/1,
         sync_feature_flags_with_cluster/2
        ]).

%% Internal use only.
-export([initialize_registry/0,
         mark_as_enabled_locally/1,
         remote_nodes/0,
         running_remote_nodes/0,
         does_node_support/3]).

%% Default timeout for operations on remote nodes.
-define(TIMEOUT, 60000).

-define(FF_REGISTRY_LOADING_LOCK, {feature_flags_registry_loading, self()}).
-define(FF_STATE_CHANGE_LOCK,     {feature_flags_state_change, self()}).

-type feature_flag_modattr() :: {feature_name(),
                                 feature_props()}.
%% The value of a `-rabbitmq_feature_flag()' module attribute used to
%% declare a new feature flag.

-type feature_name() :: atom().
%% The feature flag's name. It is used in many places to identify a
%% specific feature flag. In particular, this is how an end-user (or
%% the CLI) can enable a feature flag. This is also the only bit which
%% is persisted so a node remember which feature flags are enabled.

-type feature_props() :: #{desc => string(),
                           stability => stability(),
                           depends_on => [feature_name()],
                           migration_fun => migration_fun_name()}.
%% The feature flag properties.
%%
%% All properties are optional.
%%
%% The properties are:
%% <ul>
%% <li>`desc': a description of the feature flag</li>
%% <li>`stability': the level of stability</li>
%% <li>`depends_on': a list of feature flags name which must be enabled
%%   before this one</li>
%% <li>`migration_fun': a migration function specified by its module and
%%   function names</li>
%% </ul>
%%
%% Note that the `migration_fun' is a {@type migration_fun_name()}, not a
%% {@type migration_fun()}. However, the function signature must
%% conform to the {@type migration_fun()} signature. The reason is that
%% {@link erl_syntax:abstract/1} must be able to represent the value of
%% `migration_fun'.

-type feature_flags() :: #{feature_name() => feature_props_extended()}.
%% The feature flags map as returned or accepted by several functions in
%% this module. In particular, this what the {@link list/0} function
%% returns.

-type feature_props_extended() :: #{desc => string(),
                                    stability => stability(),
                                    migration_fun => migration_fun_name(),
                                    depends_on => [feature_name()],
                                    provided_by => atom()}.
%% The feature flag properties, once expanded by this module when feature
%% flags are discovered.
%%
%% The new properties compared to {@type feature_props()} are:
%% <ul>
%% <li>`provided_by': the name of the application providing the feature flag</li>
%% </ul>

-type stability() :: stable | experimental.
%% The level of stability of a feature flag. Currently, only informational.

-type migration_fun_name() :: {Module :: atom(), Function :: atom()}.
%% The name of the module and function to call when changing the state of
%% the feature flag.

-type migration_fun() :: fun((feature_name(),
                              feature_props_extended(),
                              migration_fun_context())
                             -> ok | {error, any()}).
%% The migration function signature.
%%
%% It is called with context `enable' when a feature flag is being enabled.
%% The function is responsible for this feature-flag-specific verification
%% and data conversion. It returns `ok' if RabbitMQ can mark the feature
%% flag as enabled an continue with the next one, if any. Otherwise, it
%% returns `{error, any()}' if there is an error and the feature flag should
%% remain disabled. The function must be idempotent: if the feature flag is
%% already enabled on another node and the local node is running this function
%% again because it is syncing its feature flags state, it should succeed.

-type migration_fun_context() :: enable.

-export_type([feature_flag_modattr/0,
              feature_name/0,
              feature_props/0,
              feature_flags/0,
              feature_props_extended/0,
              stability/0,
              migration_fun_name/0,
              migration_fun/0,
              migration_fun_context/0]).

-spec list() -> feature_flags().
%% @doc
%% Lists all supported feature flags.
%%
%% @returns A map of all supported feature flags.

list() -> list(all).

-spec list(Which :: all | enabled | disabled) -> feature_flags().
%% @doc
%% Lists all, enabled or disabled feature flags, dependening on the argument.
%%
%% @param Which The group of feature flags to return: `all', `enabled' or
%% `disabled'.
%% @returns A map of selected feature flags.

list(all)      -> rabbit_ff_registry:list(all);
list(enabled)  -> rabbit_ff_registry:list(enabled);
list(disabled) -> maps:filter(
                    fun(FeatureName, _) -> is_disabled(FeatureName) end,
                    list(all)).

-spec list(all | enabled | disabled, stability()) -> feature_flags().
%% @doc
%% Lists all, enabled or disabled feature flags, dependening on the first
%% argument, only keeping those having the specified stability.
%%
%% @param Which The group of feature flags to return: `all', `enabled' or
%% `disabled'.
%% @param Stability The level of stability used to filter the map of feature
%% flags.
%% @returns A map of selected feature flags.

list(Which, Stability)
  when Stability =:= stable orelse Stability =:= experimental ->
    maps:filter(fun(_, FeatureProps) ->
                        Stability =:= get_stability(FeatureProps)
                end, list(Which)).

-spec enable(feature_name() | [feature_name()]) -> ok |
                                                   {error, Reason :: any()}.
%% @doc
%% Enables the specified feature flag or the specified list of feature flags.
%%
%% @param FeatureName The name or the list of names of feature flags to enable.
%% @returns `ok' if the feature flags (and all the feature flags they depend
%%   on) were successfully enabled, or `{error, Reason}' if one feature flag
%%   could not be enabled (subsequent feature flags in the dependency tree are
%%   left unchanged).

enable(FeatureName) when is_atom(FeatureName) ->
    rabbit_log:debug("Feature flag `~s`: REQUEST TO ENABLE",
                     [FeatureName]),
    case is_enabled(FeatureName) of
        true ->
            rabbit_log:debug("Feature flag `~s`: already enabled",
                             [FeatureName]),
            ok;
        false ->
            rabbit_log:debug("Feature flag `~s`: not enabled, "
                             "check if supported by cluster",
                             [FeatureName]),
            %% The feature flag must be supported locally and remotely
            %% (i.e. by all members of the cluster).
            case is_supported(FeatureName) of
                true ->
                    rabbit_log:info("Feature flag `~s`: supported, "
                                    "attempt to enable...",
                                    [FeatureName]),
                    do_enable(FeatureName);
                false ->
                    rabbit_log:error("Feature flag `~s`: not supported",
                                     [FeatureName]),
                    {error, unsupported}
            end
    end;
enable(FeatureNames) when is_list(FeatureNames) ->
    with_feature_flags(FeatureNames, fun enable/1).

-spec enable_all() -> ok | {error, any()}.
%% @doc
%% Enables all supported feature flags.
%%
%% @returns `ok' if the feature flags were successfully enabled, or
%%   `{error, Reason}' if one feature flag could not be enabled
%%   (subsequent feature flags in the dependency tree are left
%%   unchanged).

enable_all() ->
    with_feature_flags(maps:keys(list(all)), fun enable/1).

-spec disable(feature_name() | [feature_name()]) -> ok | {error, any()}.

disable(FeatureName) when is_atom(FeatureName) ->
    {error, unsupported};
disable(FeatureNames) when is_list(FeatureNames) ->
    with_feature_flags(FeatureNames, fun disable/1).

-spec disable_all() -> ok | {error, any()}.

disable_all() ->
    with_feature_flags(maps:keys(list(all)), fun disable/1).

-spec with_feature_flags([feature_name()],
                         fun((feature_name()) -> ok | {error, any()})) ->
    ok | {error, any()}.
%% @private

with_feature_flags([FeatureName | Rest], Fun) ->
    case Fun(FeatureName) of
        ok    -> with_feature_flags(Rest, Fun);
        Error -> Error
    end;
with_feature_flags([], _) ->
    ok.

-spec is_supported(feature_name()) -> boolean().
%% @doc
%% Returns if a feature flag is supported by the entire cluster.
%%
%% This is the same as calling both {@link is_supported_locally/1} and
%% {@link is_supported_remotely/1} with a logical AND.
%%
%% @param FeatureName The name of the feature flag the state belongs to.

is_supported(FeatureName) when is_atom(FeatureName) ->
    is_supported_locally(FeatureName) andalso
    is_supported_remotely(FeatureName).

-spec is_supported_locally(feature_name()) -> boolean().
%% @doc
%% Returns if a feature flag is supported by the local node.
%%
%% @param FeatureName The name of the feature flag the state belongs to.

is_supported_locally(FeatureName) when is_atom(FeatureName) ->
    rabbit_ff_registry:is_supported(FeatureName).

-spec is_supported_remotely(feature_name()) -> boolean().
%% @doc
%% Returns if a feature flag is supported by all remote nodes.
%%
%% @param FeatureName The name of the feature flag the state belongs to.

is_supported_remotely(FeatureName) ->
    is_supported_remotely(FeatureName, ?TIMEOUT).

-spec is_supported_remotely(feature_name(), timeout()) -> boolean().
%% @doc
%% Returns if a feature flag is supported by all remote nodes.
%%
%% @param FeatureName The name of the feature flag the state belongs to.
%% @param Timeout The timeout to set when doing RPC calls to remote nodes.

is_supported_remotely(FeatureName, Timeout) ->
    are_supported_remotely([FeatureName], Timeout).

-spec are_supported([feature_name()]) -> boolean().

are_supported(FeatureNames) when is_list(FeatureNames) ->
    are_supported_locally(FeatureNames) andalso
    are_supported_remotely(FeatureNames).

-spec are_supported_locally([feature_name()]) -> boolean().

are_supported_locally(FeatureNames) when is_list(FeatureNames) ->
    lists:all(fun(F) -> is_supported_locally(F) end, FeatureNames).

-spec are_supported_remotely([feature_name()]) -> boolean().

are_supported_remotely(FeatureNames) when is_list(FeatureNames) ->
    are_supported_remotely(FeatureNames, ?TIMEOUT).

-spec are_supported_remotely([feature_name()], timeout()) -> boolean().

are_supported_remotely([], _) ->
    rabbit_log:debug("Feature flags: skipping query for feature flags "
                     "support as the given list is empty",
                     []),
    true;
are_supported_remotely(FeatureNames, Timeout) when is_list(FeatureNames) ->
    case running_remote_nodes() of
        [] ->
            rabbit_log:debug("Feature flags: isolated node; "
                             "skipping remote node query "
                             "=> consider `~p` supported",
                             [FeatureNames]),
            true;
        RemoteNodes ->
            rabbit_log:debug("Feature flags: about to query these remote nodes "
                             "about support for `~p`: ~p",
                             [FeatureNames, RemoteNodes]),
            are_supported_remotely(RemoteNodes, FeatureNames, Timeout)
    end.

are_supported_remotely(_, [], _) ->
    rabbit_log:debug("Feature flags: skipping query for feature flags "
                     "support as the given list is empty",
                     []),
    true;
are_supported_remotely([Node | Rest], FeatureNames, Timeout) ->
    case does_node_support(Node, FeatureNames, Timeout) of
        true ->
            are_supported_remotely(Rest, FeatureNames, Timeout);
        false ->
            rabbit_log:debug("Feature flags: stopping query "
                             "for support for `~p` here",
                             [FeatureNames]),
            false
    end;
are_supported_remotely([], FeatureNames, _) ->
    rabbit_log:info("Feature flags: all running remote nodes support `~p`",
                    [FeatureNames]),
    true.

-spec is_enabled(feature_name()) -> boolean().

is_enabled(FeatureName) when is_atom(FeatureName) ->
    is_enabled(FeatureName, blocking).

-spec is_enabled
(feature_name(), blocking) -> boolean();
(feature_name(), non_blocking) -> boolean() | state_changing.

is_enabled(FeatureName, non_blocking) ->
    rabbit_ff_registry:is_enabled(FeatureName);
is_enabled(FeatureName, blocking) ->
    case rabbit_ff_registry:is_enabled(FeatureName) of
        state_changing ->
            global:set_lock(?FF_STATE_CHANGE_LOCK),
            global:del_lock(?FF_STATE_CHANGE_LOCK),
            is_enabled(FeatureName);
        IsEnabled ->
            IsEnabled
    end.

-spec is_disabled(feature_name()) -> boolean().

is_disabled(FeatureName) when is_atom(FeatureName) ->
    is_disabled(FeatureName, blocking).

-spec is_disabled
(feature_name(), blocking) -> boolean();
(feature_name(), non_blocking) -> boolean() | state_changing.

is_disabled(FeatureName, Blocking) ->
    case is_enabled(FeatureName, Blocking) of
        state_changing -> state_changing;
        IsEnabled      -> not IsEnabled
    end.

-spec info() -> ok.

info() ->
    info(#{}).

-spec info(#{color => boolean(),
             verbose => non_neg_integer()}) -> ok.

info(Options) when is_map(Options) ->
    rabbit_ff_extra:info(Options).

-spec get_state(feature_name()) -> enabled | disabled | unavailable.

get_state(FeatureName) when is_atom(FeatureName) ->
    IsEnabled = rabbit_feature_flags:is_enabled(FeatureName),
    IsSupported = rabbit_feature_flags:is_supported(FeatureName),
    case IsEnabled of
        true  -> enabled;
        false -> case IsSupported of
                     true  -> disabled;
                     false -> unavailable
                 end
    end.

-spec get_stability(feature_name() | feature_props()) -> stability().

get_stability(FeatureName) when is_atom(FeatureName) ->
    case rabbit_ff_registry:get(FeatureName) of
        undefined    -> undefined;
        FeatureProps -> get_stability(FeatureProps)
    end;
get_stability(FeatureProps) when is_map(FeatureProps) ->
    maps:get(stability, FeatureProps, stable).

%% -------------------------------------------------------------------
%% Feature flags registry.
%% -------------------------------------------------------------------

init() ->
    _ = list(all),
    ok.

initialize_registry() ->
    EnabledFeatureNames = read_enabled_feature_flags_list(),
    initialize_registry(EnabledFeatureNames, []).

initialize_registry(EnabledFeatureNames, ChangingFeatureNames) ->
    rabbit_log:debug("Feature flags: (re)initialize registry", []),
    AllFeatureFlags = query_supported_feature_flags(),
    rabbit_log:info("Feature flags: List of feature flags found:", []),
    lists:foreach(
      fun(FeatureName) ->
              rabbit_log:info(
                "Feature flags:   [~s] ~s",
                [case lists:member(FeatureName, EnabledFeatureNames) of
                     true  -> "x";
                     false -> " "
                 end,
                 FeatureName])
      end, lists:sort(maps:keys(AllFeatureFlags))),
    regen_registry_mod(AllFeatureFlags,
                       EnabledFeatureNames,
                       ChangingFeatureNames).

query_supported_feature_flags() ->
    rabbit_log:debug("Feature flags: query feature flags in loaded applications"),
    AttributesPerApp = rabbit_misc:all_module_attributes(rabbit_feature_flag),
    query_supported_feature_flags(AttributesPerApp, #{}).

query_supported_feature_flags([{App, _Module, Attributes} | Rest],
                              AllFeatureFlags) ->
    rabbit_log:debug("Feature flags: application `~s` "
                    "has ~b feature flags",
                    [App, length(Attributes)]),
    AllFeatureFlags1 = lists:foldl(
                         fun({FeatureName, FeatureProps}, AllFF) ->
                                 merge_new_feature_flags(AllFF,
                                                         App,
                                                         FeatureName,
                                                         FeatureProps)
                         end, AllFeatureFlags, Attributes),
    query_supported_feature_flags(Rest, AllFeatureFlags1);
query_supported_feature_flags([], AllFeatureFlags) ->
    AllFeatureFlags.

merge_new_feature_flags(AllFeatureFlags, App, FeatureName, FeatureProps)
  when is_atom(FeatureName) andalso is_map(FeatureProps) ->
    FeatureProps1 = maps:put(provided_by, App, FeatureProps),
    maps:merge(AllFeatureFlags,
               #{FeatureName => FeatureProps1}).

regen_registry_mod(AllFeatureFlags,
                   EnabledFeatureNames,
                   ChangingFeatureNames) ->
    %% -module(rabbit_ff_registry).
    ModuleAttr = erl_syntax:attribute(
                   erl_syntax:atom(module),
                   [erl_syntax:atom(rabbit_ff_registry)]),
    ModuleForm = erl_syntax:revert(ModuleAttr),
    %% -export([...]).
    ExportAttr = erl_syntax:attribute(
                   erl_syntax:atom(export),
                   [erl_syntax:list(
                      [erl_syntax:arity_qualifier(
                         erl_syntax:atom(F),
                         erl_syntax:integer(A))
                       || {F, A} <- [{list, 1},
                                     {is_supported, 1},
                                     {is_enabled, 1}]]
                     )
                   ]
                  ),
    ExportForm = erl_syntax:revert(ExportAttr),
    %% get(_) -> ...
    GetClauses = [erl_syntax:clause(
                    [erl_syntax:atom(FeatureName)],
                    [],
                    [erl_syntax:abstract(maps:get(FeatureName,
                                                  AllFeatureFlags))])
                     || FeatureName <- maps:keys(AllFeatureFlags)
                    ],
    GetUnknownClause = erl_syntax:clause(
                         [erl_syntax:variable("_")],
                         [],
                         [erl_syntax:atom(undefined)]),
    GetFun = erl_syntax:function(
               erl_syntax:atom(get),
               GetClauses ++ [GetUnknownClause]),
    GetFunForm = erl_syntax:revert(GetFun),
    %% list(_) -> ...
    ListAllBody = erl_syntax:abstract(AllFeatureFlags),
    ListAllClause = erl_syntax:clause([erl_syntax:atom(all)],
                                      [],
                                      [ListAllBody]),
    EnabledFeatureFlags = maps:filter(
                            fun(FeatureName, _) ->
                                    lists:member(FeatureName,
                                                 EnabledFeatureNames)
                            end, AllFeatureFlags),
    ListEnabledBody = erl_syntax:abstract(EnabledFeatureFlags),
    ListEnabledClause = erl_syntax:clause([erl_syntax:atom(enabled)],
                                          [],
                                          [ListEnabledBody]),
    ListFun = erl_syntax:function(
                erl_syntax:atom(list),
                [ListAllClause, ListEnabledClause]),
    ListFunForm = erl_syntax:revert(ListFun),
    %% is_supported(_) -> ...
    IsSupportedClauses = [erl_syntax:clause(
                            [erl_syntax:atom(FeatureName)],
                            [],
                            [erl_syntax:atom(true)])
                          || FeatureName <- maps:keys(AllFeatureFlags)
                         ],
    NotSupportedClause = erl_syntax:clause(
                           [erl_syntax:variable("_")],
                           [],
                           [erl_syntax:atom(false)]),
    IsSupportedFun = erl_syntax:function(
                       erl_syntax:atom(is_supported),
                       IsSupportedClauses ++ [NotSupportedClause]),
    IsSupportedFunForm = erl_syntax:revert(IsSupportedFun),
    %% is_enabled(_) -> ...
    IsEnabledClauses = [erl_syntax:clause(
                          [erl_syntax:atom(FeatureName)],
                          [],
                          [case lists:member(FeatureName,
                                             ChangingFeatureNames) of
                               true ->
                                   erl_syntax:atom(state_changing);
                               false ->
                                   erl_syntax:atom(
                                     lists:member(FeatureName,
                                                  EnabledFeatureNames))
                           end])
                        || FeatureName <- maps:keys(AllFeatureFlags)
                       ],
    NotEnabledClause = erl_syntax:clause(
                         [erl_syntax:variable("_")],
                         [],
                         [erl_syntax:atom(false)]),
    IsEnabledFun = erl_syntax:function(
                     erl_syntax:atom(is_enabled),
                     IsEnabledClauses ++ [NotEnabledClause]),
    IsEnabledFunForm = erl_syntax:revert(IsEnabledFun),
    %% Compilation!
    Forms = [ModuleForm,
             ExportForm,
             GetFunForm,
             ListFunForm,
             IsSupportedFunForm,
             IsEnabledFunForm],
    CompileOpts = [return_errors,
                   return_warnings],
    case compile:forms(Forms, CompileOpts) of
        {ok, Mod, Bin, _} ->
            load_registry_mod(Mod, Bin);
        {error, Errors, Warnings} ->
            rabbit_log:error("Feature flags: registry compilation:~n"
                             "Errors: ~p~n"
                             "Warnings: ~p",
                             [Errors, Warnings]),
            {error, compilation_failure}
    end.

load_registry_mod(Mod, Bin) ->
    rabbit_log:debug("Feature flags: registry module ready, loading it..."),
    FakeFilename = "Compiled and loaded by " ++ ?MODULE_STRING,
    global:set_lock(?FF_REGISTRY_LOADING_LOCK, [node()]),
    _ = code:soft_purge(Mod),
    _ = code:delete(Mod),
    Ret = code:load_binary(Mod, FakeFilename, Bin),
    global:del_lock(?FF_REGISTRY_LOADING_LOCK, [node()]),
    case Ret of
        {module, _} ->
            rabbit_log:debug("Feature flags: registry module loaded"),
            ok;
        {error, Reason} ->
            rabbit_log:error("Feature flags: failed to load registry "
                             "module: ~p", [Reason]),
            throw({feature_flag_registry_reload_failure, Reason})
    end.

%% -------------------------------------------------------------------
%% Feature flags state storage.
%% -------------------------------------------------------------------

read_enabled_feature_flags_list() ->
    File = enabled_feature_flags_list_file(),
    case file:consult(File) of
        {ok, [List]}    -> List;
        {error, enoent} -> [];
        {error, Reason} -> {error, Reason}
    end.

write_enabled_feature_flags_list(FeatureNames) ->
    File = enabled_feature_flags_list_file(),
    Content = io_lib:format("~p.~n", [FeatureNames]),
    file:write_file(File, Content).

enabled_feature_flags_list_file() ->
    case application:get_env(rabbit, feature_flags_file) of
        {ok, Val} -> Val;
        _         -> filename:join([rabbit_mnesia:dir(), "feature_flags"])
    end.

%% -------------------------------------------------------------------
%% Feature flags management: enabling.
%% -------------------------------------------------------------------

do_enable(FeatureName) ->
    %% We mark this feature flag as "state changing" before doing the
    %% actual state change. We also take a global lock: this permits
    %% to block callers asking about a feature flag changing state.
    global:set_lock(?FF_STATE_CHANGE_LOCK),
    EnabledFeatureNames = maps:keys(list(enabled)),
    Ret = case initialize_registry(EnabledFeatureNames, [FeatureName]) of
              ok ->
                  case enable_dependencies(FeatureName, true) of
                      ok ->
                          case run_migration_fun(FeatureName, enable) of
                              ok    -> mark_as_enabled(FeatureName);
                              Error -> Error
                          end;
                      Error ->
                          Error
                  end;
              Error ->
                  Error
          end,
    case Ret of
        ok -> ok;
        _  -> initialize_registry(EnabledFeatureNames, [])
    end,
    global:del_lock(?FF_STATE_CHANGE_LOCK),
    Ret.

enable_locally(FeatureName) when is_atom(FeatureName) ->
    case is_enabled(FeatureName) of
        true ->
            ok;
        false ->
            rabbit_log:debug(
              "Feature flag `~s`: enable locally (i.e. was enabled on the cluster "
              "when this node was not part of it)",
              [FeatureName]),
            do_enable_locally(FeatureName)
    end.

do_enable_locally(FeatureName) ->
    case enable_dependencies(FeatureName, false) of
        ok ->
            case run_migration_fun(FeatureName, enable) of
                ok    -> mark_as_enabled_locally(FeatureName);
                Error -> Error
            end;
        Error -> Error
    end.

enable_dependencies(FeatureName, Everywhere) ->
    #{FeatureName := FeatureProps} = rabbit_ff_registry:list(all),
    DependsOn = maps:get(depends_on, FeatureProps, []),
    rabbit_log:debug("Feature flag `~s`: enable dependencies: ~p",
                     [FeatureName, DependsOn]),
    enable_dependencies(FeatureName, DependsOn, Everywhere).

enable_dependencies(TopLevelFeatureName, [FeatureName | Rest], Everywhere) ->
    Ret = case Everywhere of
              true  -> enable(FeatureName);
              false -> enable_locally(FeatureName)
          end,
    case Ret of
        ok    -> enable_dependencies(TopLevelFeatureName, Rest, Everywhere);
        Error -> Error
    end;
enable_dependencies(_, [], _) ->
    ok.

run_migration_fun(FeatureName, Arg) ->
    #{FeatureName := FeatureProps} = rabbit_ff_registry:list(all),
    case maps:get(migration_fun, FeatureProps, none) of
        {MigrationMod, MigrationFun}
          when is_atom(MigrationMod) andalso is_atom(MigrationFun) ->
            rabbit_log:debug("Feature flag `~s`: run migration function ~p "
                             "with arg: ~p",
                             [FeatureName, MigrationFun, Arg]),
            try
                erlang:apply(MigrationMod,
                             MigrationFun,
                             [FeatureName, FeatureProps, Arg])
            catch
                _:Reason:Stacktrace ->
                    rabbit_log:error("Feature flag `~s`: migration function "
                                     "crashed: ~p~n~p",
                                     [FeatureName, Reason, Stacktrace]),
                    {error, {migration_fun_crash, Reason, Stacktrace}}
            end;
        none ->
            ok;
        Invalid ->
            rabbit_log:error("Feature flag `~s`: invalid migration "
                             "function: ~p",
                             [FeatureName, Invalid]),
            {error, {invalid_migration_fun, Invalid}}
    end.

mark_as_enabled(FeatureName) ->
    ok = mark_as_enabled_locally(FeatureName),
    ok = mark_as_enabled_remotely(FeatureName).

mark_as_enabled_locally(FeatureName) ->
    rabbit_log:info("Feature flag `~s`: mark as enabled",
                    [FeatureName]),
    EnabledFeatureNames = lists:usort(
                            [FeatureName | maps:keys(list(enabled))]),
    write_enabled_feature_flags_list(EnabledFeatureNames),
    initialize_registry(EnabledFeatureNames, []).

mark_as_enabled_remotely(FeatureName) ->
    %% FIXME: Handle error cases.
    [ok = rpc:call(Node, ?MODULE, mark_as_enabled_locally, [FeatureName], ?TIMEOUT)
     || Node <- running_remote_nodes()],
    ok.

%% -------------------------------------------------------------------
%% Coordination with remote nodes.
%% -------------------------------------------------------------------

remote_nodes() ->
    mnesia:system_info(db_nodes) -- [node()].

running_remote_nodes() ->
    mnesia:system_info(running_db_nodes) -- [node()].

does_node_support(Node, FeatureNames, Timeout) ->
    rabbit_log:debug("Feature flags: querying `~p` support on node ~s...",
                     [FeatureNames, Node]),
    Ret = case node() of
              Node ->
                  are_supported_locally(FeatureNames);
              _ ->
                  rpc:call(Node,
                           ?MODULE, are_supported_locally, [FeatureNames],
                           Timeout)
          end,
    case Ret of
        {badrpc, {'EXIT',
                  {undef,
                   [{?MODULE, are_supported_locally, [FeatureNames], []}
                    | _]}}} ->
            rabbit_log:debug(
              "Feature flags: ?MODULE:are_supported_locally(~p) unavailable "
              "on node `~s`: assuming it is a RabbitMQ 3.7.x node "
              "=> consider the feature flags unsupported",
              [FeatureNames, Node]),
            false;
        {badrpc, Reason} ->
            rabbit_log:error("Feature flags: error while querying `~p` "
                             "support on node ~s: ~p",
                             [FeatureNames, Node, Reason]),
            false;
        true ->
            rabbit_log:debug("Feature flags: node `~s` supports `~p`",
                             [Node, FeatureNames]),
            true;
        false ->
            rabbit_log:debug("Feature flags: node `~s` does not support `~p`; "
                             "stopping query here",
                             [Node, FeatureNames]),
            false
    end.

check_node_compatibility(Node) ->
    check_node_compatibility(Node, ?TIMEOUT).

check_node_compatibility(Node, Timeout) ->
    rabbit_log:debug("Feature flags: node `~s` compatibility check, part 1/2",
                     [Node]),
    Part1 = local_enabled_feature_flags_are_supported_remotely(Node, Timeout),
    rabbit_log:debug("Feature flags: node `~s` compatibility check, part 2/2",
                     [Node]),
    Part2 = remote_enabled_feature_flags_are_supported_locally(Node, Timeout),
    case {Part1, Part2} of
        {true, true} ->
            rabbit_log:debug("Feature flags: node `~s` is compatible", [Node]),
            ok;
        {false, _} ->
            rabbit_log:error("Feature flags: node `~s` is INCOMPATIBLE: "
                             "feature flags enabled locally are not "
                             "supported remotely",
                             [Node]),
            {error, incompatible_feature_flags};
        {_, false} ->
            rabbit_log:error("Feature flags: node `~s` is INCOMPATIBLE: "
                             "feature flags enabled remotely are not "
                             "supported locally",
                             [Node]),
            {error, incompatible_feature_flags}
    end.

is_node_compatible(Node) ->
    is_node_compatible(Node, ?TIMEOUT).

is_node_compatible(Node, Timeout) ->
    check_node_compatibility(Node, Timeout) =:= ok.

local_enabled_feature_flags_are_supported_remotely(Node, Timeout) ->
    LocalEnabledFeatureNames = maps:keys(list(enabled)),
    are_supported_remotely([Node], LocalEnabledFeatureNames, Timeout).

remote_enabled_feature_flags_are_supported_locally(Node, Timeout) ->
    case query_remote_feature_flags(Node, enabled, Timeout) of
        {error, _} ->
            false;
        RemoteEnabledFeatureFlags when is_map(RemoteEnabledFeatureFlags) ->
            RemoteEnabledFeatureNames = maps:keys(RemoteEnabledFeatureFlags),
            are_supported_locally(RemoteEnabledFeatureNames)
    end.

query_remote_feature_flags(Node, Which, Timeout) ->
    rabbit_log:debug("Feature flags: querying ~s feature flags "
                     "on node `~s`...",
                     [Which, Node]),
    case rpc:call(Node, ?MODULE, list, [Which], Timeout) of
        {badrpc, {'EXIT',
                  {undef,
                   [{?MODULE, list, [Which], []}
                    | _]}}} ->
            rabbit_log:debug(
              "Feature flags: ?MODULE:list(~s) unavailable on node `~s`: "
              "assuming it is a RabbitMQ 3.7.x node "
              "=> consider the list empty",
              [Which, Node]),
            #{};
        {badrpc, Reason} = Error ->
            rabbit_log:error(
              "Feature flags: error while querying ~s feature flags "
              "on node `~s`: ~p",
              [Which, Node, Reason]),
            {error, Error};
        RemoteFeatureFlags when is_map(RemoteFeatureFlags) ->
            RemoteFeatureNames = maps:keys(RemoteFeatureFlags),
            rabbit_log:debug("Feature flags: querying ~s feature flags "
                             "on node `~s` done; ~s features: ~p",
                             [Which, Node, Which, RemoteFeatureNames]),
            RemoteFeatureFlags
    end.

sync_feature_flags_with_cluster(Nodes) ->
    sync_feature_flags_with_cluster(Nodes, ?TIMEOUT).

sync_feature_flags_with_cluster([], _) ->
    FeatureFlags = get_forced_feature_flags_from_env(),
    case remote_nodes() of
        [] when FeatureFlags =:= undefined ->
            rabbit_log:debug(
              "Feature flags: starting an unclustered node: "
              "all feature flags will be enabled by default"),
            enable_all();
        [] ->
            case FeatureFlags of
                [] ->
                    rabbit_log:debug(
                      "Feature flags: starting an unclustered node: "
                      "all feature flags are forcibly left disabled "
                      "from the RABBITMQ_FEATURE_FLAGS environment "
                      "variable");
                _ ->
                    rabbit_log:debug(
                      "Feature flags: starting an unclustered node: "
                      "only the following feature flags specified in "
                      "the RABBITMQ_FEATURE_FLAGS environment variable "
                      "will be enabled: ~p",
                      [FeatureFlags])
            end,
            enable(FeatureFlags);
        _ ->
            ok
    end;
sync_feature_flags_with_cluster(Nodes, Timeout) ->
    RemoteNodes = Nodes -- [node()],
    sync_feature_flags_with_cluster1(RemoteNodes, Timeout).

sync_feature_flags_with_cluster1([], _) ->
    ok;
sync_feature_flags_with_cluster1(RemoteNodes, Timeout) ->
    RandomRemoteNode = pick_one_node(RemoteNodes),
    rabbit_log:debug("Feature flags: SYNCING FEATURE FLAGS with node `~s`...",
                     [RandomRemoteNode]),
    case query_remote_feature_flags(RandomRemoteNode, enabled, Timeout) of
        {error, _} = Error ->
            Error;
        RemoteFeatureFlags ->
            RemoteFeatureNames = maps:keys(RemoteFeatureFlags),
            do_sync_feature_flags_with_node1(RemoteFeatureNames)
    end.

pick_one_node(Nodes) ->
    RandomIndex = rand:uniform(length(Nodes)),
    lists:nth(RandomIndex, Nodes).

do_sync_feature_flags_with_node1([FeatureFlag | Rest]) ->
    case enable_locally(FeatureFlag) of
        ok    -> do_sync_feature_flags_with_node1(Rest);
        Error -> Error
    end;
do_sync_feature_flags_with_node1([]) ->
    ok.

get_forced_feature_flags_from_env() ->
    case os:getenv("RABBITMQ_FEATURE_FLAGS") of
        false -> undefined;
        Value -> [list_to_atom(V) ||V <- string:lexemes(Value, ",")]
    end.
