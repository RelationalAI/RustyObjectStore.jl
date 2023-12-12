using ReTestItems
using ObjectStore

withenv("RUST_BACKTRACE"=>1) do
    runtests(ObjectStore, testitem_timeout=30, nworkers=1)
end