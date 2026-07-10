package qsbridge

import "testing"

func TestDefaultMetadataStoreProfilesDescribeCoreDomains(t *testing.T) {
	profiles := DefaultMetadataStoreProfiles()
	for _, domain := range []MetadataStoreDomain{
		MetadataStoreDomainCatalog,
		MetadataStoreDomainDictionary,
		MetadataStoreDomainAccess,
		MetadataStoreDomainSession,
		MetadataStoreDomainPrepared,
	} {
		if _, ok := metadataStoreProfileByDomain(profiles, domain); !ok {
			t.Fatalf("profiles = %#v, missing domain %s", profiles, domain)
		}
	}

	catalog, _ := metadataStoreProfileByDomain(profiles, MetadataStoreDomainCatalog)
	if catalog.Backend != MetadataStoreBackendConsul || !catalog.CacheRequired || catalog.CacheScope != MetadataCacheScopeProcess ||
		catalog.Invalidation != MetadataInvalidationWatch || !catalog.Distributed {
		t.Fatalf("catalog profile = %#v, want cached distributed Consul metadata", catalog)
	}
	dictionary, _ := metadataStoreProfileByDomain(profiles, MetadataStoreDomainDictionary)
	if dictionary.Backend != MetadataStoreBackendKVStore || !dictionary.CacheRequired ||
		dictionary.CacheScope != MetadataCacheScopeProcess || dictionary.Invalidation != MetadataInvalidationVersioned || !dictionary.Mutable {
		t.Fatalf("dictionary profile = %#v, want mutable cached KVStore dictionary metadata", dictionary)
	}
}

func TestDefaultMetadataStoreProfilesReturnCopies(t *testing.T) {
	first := DefaultMetadataStoreProfiles()
	first[0].Name = "mutated"

	second := DefaultMetadataStoreProfiles()
	if second[0].Name == "mutated" {
		t.Fatalf("metadata store profiles leaked mutable state: %#v", second[0])
	}
}

func metadataStoreProfileByDomain(profiles []MetadataStoreProfile, domain MetadataStoreDomain) (MetadataStoreProfile, bool) {
	for _, profile := range profiles {
		if profile.Domain == domain {
			return profile, true
		}
	}
	return MetadataStoreProfile{}, false
}
