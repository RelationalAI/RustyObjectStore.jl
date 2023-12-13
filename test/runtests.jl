using ReTestItems
using ObjectStore

withenv("RUST_BACKTRACE"=>1) do
    runtests(ObjectStore, testitem_timeout=120, nworkers=1)
end