using ReTestItems
using RustyObjectStore

withenv("RUST_BACKTRACE"=>1) do
    runtests(RustyObjectStore; testitem_timeout=180, nworkers=1)
end
