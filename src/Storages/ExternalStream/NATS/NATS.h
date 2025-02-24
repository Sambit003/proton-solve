#pragma once

#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <NATS/NATS.h>

namespace DB
{

namespace ExternalStream
{

class NATS final : public StorageExternalStreamImpl
{
public:
    NATS(
        IStorage * storage,
        std::unique_ptr<ExternalStreamSettings> settings_,
        const ASTs & engine_args_,
        bool attach,
        ExternalStreamCounterPtr external_stream_counter_,
        ContextPtr context);
    ~NATS() override;

    String getName() const override { return "NATSExternalStream"; }

    void startup() override;
    void shutdown() override;

    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

private:
    natsConnection * conn;
    natsSubscription * sub;
    natsOptions * opts;
    ExternalStreamCounterPtr external_stream_counter;
};

}

}
