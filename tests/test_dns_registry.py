from deploy.dns_registry import DnsPeerRegistry
from deploy.identity import identity_from_env

ENV = {
    "PEERCHAT_POD_NAME": "peerchat-1",
    "PEERCHAT_SERVICE": "peerchat-hl",
    "PEERCHAT_NAMESPACE": "peerchat",
}


class _Rec:
    def __init__(self, target, port):
        self.target = target
        self.port = port


class _FakeResolver:
    def __init__(self, records):
        self._records = records
        self.queries = []

    def resolve(self, qname, rdtype):
        self.queries.append((qname, rdtype))
        return self._records


def _identity():
    return identity_from_env(ENV)


def test_srv_lookup_yields_stable_pod_names_and_excludes_self():
    base = "peerchat-{}.peerchat-hl.peerchat.svc.cluster.local"
    resolver = _FakeResolver([_Rec(base.format(i) + ".", 5678) for i in range(3)])
    reg = DnsPeerRegistry(_identity(), replicas=3, resolver=resolver)

    peers = reg.get_peers()

    assert peers == [(base.format(0), 5678), (base.format(2), 5678)]
    assert resolver.queries[0][0] == "_chat._tcp.peerchat-hl.peerchat.svc.cluster.local"
    assert resolver.queries[0][1] == "SRV"


def test_results_are_cached_between_calls():
    resolver = _FakeResolver([_Rec("peerchat-0.peerchat-hl.peerchat.svc.cluster.local.", 5678)])
    reg = DnsPeerRegistry(_identity(), replicas=2, resolver=resolver, refresh_interval=60)

    reg.get_peers()
    reg.get_peers()

    assert len(resolver.queries) == 1, "get_peers runs on every forward; must not re-query DNS"


def test_dns_failure_returns_last_known_peers():
    class _Flaky:
        def __init__(self):
            self.calls = 0

        def resolve(self, qname, rdtype):
            self.calls += 1
            if self.calls == 1:
                return [_Rec("peerchat-0.peerchat-hl.peerchat.svc.cluster.local.", 5678)]
            raise RuntimeError("SERVFAIL")

    reg = DnsPeerRegistry(_identity(), replicas=2, resolver=_Flaky(), refresh_interval=0)
    first = reg.get_peers()
    assert len(first) == 1

    # A resolver blip must not empty the peer list mid-broadcast.
    assert reg.get_peers() == first


def test_registry_supplies_no_pubkeys():
    reg = DnsPeerRegistry(_identity(), replicas=2, resolver=_FakeResolver([]))
    assert reg.get_pub_key("peerchat-0.peerchat-hl.peerchat.svc.cluster.local", 5678) == ""
