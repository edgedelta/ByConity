#pragma once

#include <Core/Types.h>

namespace DB
{

inline constexpr double PARTITION_ORDER_UNKNOWN_SELECTIVITY = -1.0;

/// Cost-based decision for partition-order reading. It pays off only when the newest partition
/// alone is estimated to satisfy the LIMIT; otherwise it descends partitions serially and loses to the
/// parallel merge. So the gate is: a tight LIMIT exists AND est(rows in newest partition) >= LIMIT.
///
///   limit            : ORDER BY ... LIMIT N (0 = no limit -> early termination impossible)
///   selectivity      : estimated fraction of rows passing the filter, in [0, 1];
///                      a NEGATIVE value means "unknown" (e.g. full-text hasToken/GIN with no token stats)
///   row_count        : estimated total table rows
///   partitions       : actual pruned partition count, supplied by the caller (ReadFromMergeTree's selected_partitions)
///   fulltext_default : decision to use when selectivity is unknown
///   safety           : multiplier on LIMIT; >1 makes the gate more conservative
///
inline bool partitionOrderGate(
    size_t limit,
    double selectivity,
    UInt64 row_count,
    size_t partitions,
    bool fulltext_default,
    double safety)
{
    /// No LIMIT -> no early-termination opportunity -> partition-order can only lose.
    if (limit == 0)
        return false;

    /// Unknown selectivity: defer to the full-text policy.
    if (selectivity < 0.0)
        return fulltext_default;

    if (partitions == 0 || row_count == 0)
        return false;

    /// Approximate rows in the newest partition assuming the matches are roughly time-uniform.
    const double est_newest_rows = selectivity * static_cast<double>(row_count) / static_cast<double>(partitions);
    return est_newest_rows >= static_cast<double>(limit) * safety;
}

}
