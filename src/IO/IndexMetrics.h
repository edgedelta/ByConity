#pragma once

#include <cstddef>
#include <common/types.h>

#include <Protos/plan_segment_manager.pb.h>

namespace DB
{

/** Progress of query execution.
  * Values, transferred over network are deltas - how much was done after previously sent value.
  * The same struct is also used for summarized values.
  */

struct IndexMetricValues
{
    std::string index_name;
    std::string index_description;
    size_t total_parts;
    size_t total_granules;
    size_t skipped_parts;
    size_t skipped_granules;

    Protos::IndexMetric toProto() const;
    void fromProto(const Protos::IndexMetric & index_metric);

    void add(const IndexMetricValues & other);
    std::string toString() const;
};

class IndexMetricsCollection
{

private:
    /** These metrics are emitted by each segment. Received values must be summed to get estimate of total skipped parts,
     *  granules etc. 
     */
    std::unordered_map<std::string, IndexMetricValues> index_metrics;
    mutable std::mutex mutex; // Mutex to protect access to metrics

public:
    IndexMetricsCollection() = default;

     // Copy constructor
    IndexMetricsCollection(const IndexMetricsCollection& other) {
        // Deep copy of the unordered_map because IndexMetricValues contains only primitive types
        index_metrics = other.index_metrics;
    }

    Protos::IndexMetrics toProto() const;
    void fromProto(const Protos::IndexMetrics & index_metrics);
   // Aggregate metrics from another collection
    void aggregate(const IndexMetricsCollection & other);
    // add if not exists in the collection
    void addOrUpdateIndexMetric(const IndexMetricValues & metric);
    std::unordered_map<std::string, IndexMetricValues> getSnapshot() const;
    void reset();
};
}
