from support.weaver_test import weaver_test


@weaver_test()
def test_power_bi_tokens_use_one_injected_credential_with_distinct_audiences():
    from weaver.fabric import auth

    requested = []

    class Credential:
        def get_token(self, scope):
            from types import SimpleNamespace

            requested.append(scope)
            return SimpleNamespace(token="recorded-token", expires_on=1)

    supplied = Credential()
    fabric = auth.TokenProvider(auth.FABRIC_SCOPE, cred=supplied)
    semantic = auth.TokenProvider(auth.POWER_BI_SCOPE, cred=supplied)
    fabric()
    semantic()
    assert requested == [
        "https://api.fabric.microsoft.com/.default",
        "https://analysis.windows.net/powerbi/api/.default",
    ]
    assert fabric._credential() is supplied
    assert semantic._credential() is supplied
