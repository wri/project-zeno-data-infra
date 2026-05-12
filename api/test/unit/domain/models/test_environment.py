from app.domain.models.environment import Environment, resolve_uris

PROD_URIS = {
    "alpha_zarr_uri": "s3://prod/alpha.zarr",
    "beta_zarr_uri": "s3://prod/beta.zarr",
    "gamma_zarr_uri": "s3://prod/gamma.zarr",
}

TABLE: dict[Environment, dict[str, str]] = {
    Environment.production: PROD_URIS,
    Environment.staging: {
        "alpha_zarr_uri": "s3://staging/alpha.zarr",  # override
        # beta and gamma fall through to production
    },
}


class TestProduction:
    def test_returns_own_uris(self):
        result = resolve_uris(TABLE, Environment.production)
        assert result["alpha_zarr_uri"] == "s3://prod/alpha.zarr"
        assert result["beta_zarr_uri"] == "s3://prod/beta.zarr"
        assert result["gamma_zarr_uri"] == "s3://prod/gamma.zarr"

    def test_does_not_inherit_staging_overrides(self):
        """Production must never be contaminated by a child environment's URIs."""
        result = resolve_uris(TABLE, Environment.production)
        assert result["alpha_zarr_uri"] == "s3://prod/alpha.zarr"

    def test_returns_copy_not_original_dict(self):
        """Mutating the result must not affect the source table."""
        result = resolve_uris(TABLE, Environment.production)
        result["alpha_zarr_uri"] = "s3://mutated/alpha.zarr"
        assert TABLE[Environment.production]["alpha_zarr_uri"] == "s3://prod/alpha.zarr"


class TestStaging:
    def test_overridden_key_uses_staging_uri(self):
        result = resolve_uris(TABLE, Environment.staging)
        assert result["alpha_zarr_uri"] == "s3://staging/alpha.zarr"

    def test_non_overridden_keys_fall_through_to_production(self):
        result = resolve_uris(TABLE, Environment.staging)
        assert result["beta_zarr_uri"] == "s3://prod/beta.zarr"
        assert result["gamma_zarr_uri"] == "s3://prod/gamma.zarr"

    def test_empty_staging_dict_fully_inherits_production(self):
        table = {Environment.production: PROD_URIS, Environment.staging: {}}
        result = resolve_uris(table, Environment.staging)
        assert result == PROD_URIS

    def test_staging_only_key_is_present(self):
        """A key introduced in staging (not in production) should be included."""
        table = {
            Environment.production: {"alpha_zarr_uri": "s3://prod/alpha.zarr"},
            Environment.staging: {"new_zarr_uri": "s3://staging/new.zarr"},
        }
        result = resolve_uris(table, Environment.staging)
        assert result["new_zarr_uri"] == "s3://staging/new.zarr"
        assert result["alpha_zarr_uri"] == "s3://prod/alpha.zarr"


class TestEdgeCases:
    def test_result_contains_all_keys_from_full_chain(self):
        """The merged dict must contain the union of all keys in the chain,
        not just the keys present in the requested environment's own dict."""
        result = resolve_uris(TABLE, Environment.staging)
        assert set(result.keys()) == {
            "alpha_zarr_uri",
            "beta_zarr_uri",
            "gamma_zarr_uri",
        }
