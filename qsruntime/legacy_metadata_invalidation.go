package qsruntime

import "github.com/QuantaStream/quantastream/shared"

// LegacySchemaChangeListener adapts legacy Consul schema watch events to qsruntime invalidation.
func LegacySchemaChangeListener(invalidator RuntimeMetadataInvalidator) shared.SchemaChangeListener {
	return func(event shared.SchemaChangeEvent) {
		invalidator.ApplyChange(LegacyMetadataChangeEvent(event))
	}
}

// LegacyMetadataChangeEvent converts a shared schema watch event to neutral metadata vocabulary.
func LegacyMetadataChangeEvent(event shared.SchemaChangeEvent) MetadataChangeEvent {
	return MetadataChangeEvent{
		Table: event.Table,
		Kind:  legacyMetadataChangeKind(event.Event),
	}
}

func legacyMetadataChangeKind(event shared.EventType) MetadataChangeKind {
	switch event {
	case shared.Create:
		return MetadataChangeCreate
	case shared.Drop:
		return MetadataChangeDrop
	default:
		return MetadataChangeModify
	}
}
