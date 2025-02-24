#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/NATS/NATS.h>
#include <Storages/ExternalStream/parseShards.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <filesystem>
#include <optional>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
extern const int INVALID_SETTING_VALUE;
extern const int NO_AVAILABLE_NATS_CONSUMER;
}

namespace
{

const String MAX_CONSUMERS_CONFIG_KEY = "external_stream.nats.max_consumers_per_stream";
const size_t DEFAULT_MAX_CONSUMERS = 50;

NATS::ConfPtr createConfFromSettings(const NATSExternalStreamSettings & settings)
{
    if (settings.nats_url.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `nats_url` setting for NATS external stream");

    NATS::ConfPtr conf{natsOptions_Create(), natsOptions_Destroy};
    char errstr[512]{'\0'};

    auto conf_set = [&](const String & name, const String & value) {
        auto err = natsOptions_SetURL(conf.get(), value.c_str());
        if (err != NATS_OK)
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Failed to set NATS config `{}` with value `{}` error_code={} error_msg={}",
                name,
                value,
                err,
                errstr);
        }
    };

    conf_set("url", settings.nats_url.value);

    if (!settings.nats_tls_cert.value.empty() && !settings.nats_tls_key.value.empty() && !settings.nats_tls_ca.value.empty())
    {
        conf_set("tls_cert", settings.nats_tls_cert.value);
        conf_set("tls_key", settings.nats_tls_key.value);
        conf_set("tls_ca", settings.nats_tls_ca.value);
    }

    return conf;
}

}

NATS::ConfPtr NATS::createNATSConf(NATSExternalStreamSettings settings_)
{
    return createConfFromSettings(settings_);
}

NATS::NATS(
    IStorage * storage,
    std::unique_ptr<ExternalStreamSettings> settings_,
    const ASTs & engine_args_,
    bool attach,
    ExternalStreamCounterPtr external_stream_counter_,
    ContextPtr context)
    : StorageExternalStreamImpl(storage, std::move(settings_), context)
    , engine_args(engine_args_)
    , external_stream_counter(external_stream_counter_)
    , conf(createNATSConf(settings->getNATSSettings()))
    , max_consumers(context->getConfigRef().getInt(MAX_CONSUMERS_CONFIG_KEY, DEFAULT_MAX_CONSUMERS))
{
    assert(settings->type.value == StreamTypes::NATS);
    assert(external_stream_counter);

    if (settings->nats_subject.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `nats_subject` setting for NATS external stream");

    cacheVirtualColumnNamesAndTypes();

    if (!attach)
        validate();
}

void NATS::validate()
{
    auto consumer = getConsumer();
    if (!consumer)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Failed to create NATS consumer");
}

NamesAndTypesList NATS::getVirtuals() const
{
    return virtual_column_names_and_types;
}

void NATS::cacheVirtualColumnNamesAndTypes()
{
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_EVENT_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_SHARD, std::make_shared<DataTypeInt32>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_MESSAGE_KEY, std::make_shared<DataTypeString>()));

    DataTypes header_types{/*key_type*/ std::make_shared<DataTypeString>(), /*value_type*/ std::make_shared<DataTypeString>()};
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_MESSAGE_HEADERS, std::make_shared<DataTypeMap>(header_types)));
}

std::shared_ptr<natsConnection> NATS::getConsumer()
{
    std::lock_guard<std::mutex> lock{consumer_mutex};

    auto consumer_ref = std::find_if(consumers.begin(), consumers.end(), [](const auto & consumer) { return consumer.expired(); });
    if (consumer_ref == consumers.end() && consumers.size() >= max_consumers)
        throw Exception(
            ErrorCodes::NO_AVAILABLE_NATS_CONSUMER,
            "Reached consumers limit {}. Existing queries need to be stopped before running other queries. Or update {} to a bigger number "
            "in the config file",
            max_consumers,
            MAX_CONSUMERS_CONFIG_KEY);

    auto new_consumer = std::make_shared<natsConnection>();
    std::weak_ptr<natsConnection> ref = new_consumer;

    if (consumer_ref != consumers.end())
        consumer_ref->swap(ref);
    else
        consumers.push_back(ref);

    return new_consumer;
}

Pipe NATS::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    auto consumer = getConsumer();

    auto header = storage_snapshot->getSampleBlockForColumns(column_names);

    auto streaming = query_info.syntax_analyzer_result->streaming;

    LOG_INFO(logger, "Reading NATS subject={} streaming={}", settings->nats_subject.value, streaming);

    Pipes pipes;
    pipes.reserve(1);

    {
        auto seek_to_info = query_info.seek_to_info;
        if (!streaming && seek_to_info->getSeekTo().empty())
            seek_to_info = std::make_shared<SeekToInfo>("earliest");

        pipes.emplace_back(std::make_shared<NATSSource>(
            *this,
            header,
            storage_snapshot,
            consumer,
            settings->nats_subject.value,
            max_block_size,
            external_stream_counter,
            &Poco::Logger::get(fmt::format("{}.{}", getLoggerName(), consumer->name())),
            context));
    }

    LOG_INFO(
        logger,
        "Starting reading {} streams by seeking to {} in dedicated resource group",
        pipes.size(),
        query_info.seek_to_info->getSeekTo());

    auto pipe = Pipe::unitePipes(std::move(pipes));
    auto min_threads = context->getSettingsRef().min_threads.value;
    if (min_threads > 1)
        pipe.resize(min_threads);

    return pipe;
}

SinkToStoragePtr NATS::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    validate();
    return std::make_shared<NATSSink>(
        *this,
        metadata_snapshot->getSampleBlock(),
        external_stream_counter,
        &Poco::Logger::get(fmt::format("{}.{}", getLoggerName(), getProducer()->name())),
        context);
}

std::shared_ptr<natsConnection> NATS::getProducer()
{
    if (producer)
        return producer;

    std::lock_guard<std::mutex> lock{producer_mutex};
    if (producer)
        return producer;

    auto producer_ptr = std::make_shared<natsConnection>();
    producer.swap(producer_ptr);

    return producer;
}

}
