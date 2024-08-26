#include <atomic>
#include <fmt/core.h>

#include "IndexMetrics.h"
#include <Protos/plan_segment_manager.pb.h>
#include <common/logger_useful.h>

namespace DB
{

Protos::IndexMetric IndexMetricValues::toProto() const
{
    Protos::IndexMetric proto;
    proto.set_index_name(this->index_name);
    proto.set_index_description(this->index_description);
    proto.set_total_parts(this->total_parts);
    proto.set_total_granules(this->total_granules);
    proto.set_skipped_parts(this->skipped_parts);
    proto.set_skipped_granules(this->skipped_granules);
    return proto;
}

void IndexMetricValues::fromProto(const Protos::IndexMetric & index_metrics)
{
    this->index_name = index_metrics.index_name();
    this->index_description = index_metrics.index_description();
    this->total_parts = index_metrics.total_parts();
    this->total_granules = index_metrics.total_granules();
    this->skipped_parts = index_metrics.skipped_parts();
    this->skipped_granules = index_metrics.skipped_granules();
}

void IndexMetricValues::add(const IndexMetricValues & other)
{
    total_parts += other.total_parts;
    total_granules += other.total_granules;
    skipped_parts += other.skipped_parts;
    skipped_granules += other.skipped_granules;
}

String IndexMetricValues::toString() const
{
    return fmt::format(
        "index metrics[index_name={}, index_description={}, skipped_parts={}/{}, skipped_granules={}/{}]",
        index_name,
        index_description,
        skipped_parts,
        total_parts,
        skipped_granules,
        total_granules);
}

Protos::IndexMetrics IndexMetricsCollection::toProto() const
{
    std::lock_guard<std::mutex> lock(mutex);
    Protos::IndexMetrics proto;
    for (const auto & pair : index_metrics)
    {
        auto * proto_metric = proto.add_index_metrics();
        *proto_metric = pair.second.toProto();
    }
    
    return proto;
} 

void IndexMetricsCollection::fromProto(const Protos::IndexMetrics & proto)
{
    index_metrics.clear();
    for (const auto & proto_metric : proto.index_metrics())
    {
        IndexMetricValues metric;
        metric.fromProto(proto_metric);
        addOrUpdateIndexMetric(metric);
    }
}

void IndexMetricsCollection::addOrUpdateIndexMetric(const IndexMetricValues & metric_values)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto it = index_metrics.find(metric_values.index_name);
    if (it != index_metrics.end())
    {
        it->second.add(metric_values);
    }
    else
    {
        index_metrics[metric_values.index_name] = metric_values;
    }
}

// Aggregate metrics from another collection
void IndexMetricsCollection::aggregate(const IndexMetricsCollection & other)
{
    for (const auto & pair : other.index_metrics)
    {
        addOrUpdateIndexMetric(pair.second);
    }
}

std::unordered_map<std::string, IndexMetricValues> IndexMetricsCollection::getSnapshot() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return index_metrics; // Returns a copy of the metrics for thread-safe access
}
}
