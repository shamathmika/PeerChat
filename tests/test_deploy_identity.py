import pytest

from deploy.identity import IdentityError, identity_from_env

ENV = {
    "PEERCHAT_POD_NAME": "peerchat-3",
    "PEERCHAT_SERVICE": "peerchat-hl",
    "PEERCHAT_NAMESPACE": "peerchat",
    "PEERCHAT_PORT": "5678",
}


def test_identity_from_statefulset_ordinal():
    ident = identity_from_env(ENV)
    assert ident.ordinal == 3
    assert ident.fqdn == "peerchat-3.peerchat-hl.peerchat.svc.cluster.local"
    assert ident.address == "peerchat-3.peerchat-hl.peerchat.svc.cluster.local:5678"


def test_identity_is_deterministic_across_restarts():
    """Same pod name -> same vector-clock key, which is the whole point."""
    assert identity_from_env(ENV).address == identity_from_env(dict(ENV)).address


def test_sibling_fqdn_by_ordinal():
    ident = identity_from_env(ENV)
    assert ident.peer_fqdn(0) == "peerchat-0.peerchat-hl.peerchat.svc.cluster.local"


def test_non_statefulset_pod_name_rejected():
    with pytest.raises(IdentityError):
        identity_from_env({**ENV, "PEERCHAT_POD_NAME": "peerchat-deadbeef"})


def test_missing_service_rejected():
    with pytest.raises(IdentityError):
        identity_from_env({"PEERCHAT_POD_NAME": "peerchat-0"})
