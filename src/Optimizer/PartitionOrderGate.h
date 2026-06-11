#pragma once

#include <Core/Types.h>

namespace DB
{

inline constexpr double PARTITION_ORDER_UNKNOWN_SELECTIVITY = -1.0;

/// Cost-based decision for partition-order reading. It pays off only when the newest partition
/// alone is estimated to satisfy the LIMIT; otherwise it descends partitions serially and loses to the
/// parallel merge. So the gate is: a tight LIMIT exists AND est(matching rows in newest partition) >= LIMIT.
///
///   limit            : ORDER BY ... LIMIT N (0 = no limit -> early termination impossible)
///   selectivity      : estimated fraction of rows passing the filter, in [0, 1];
///                      a NEGATIVE value means "unknown" (e.g. full-text hasToken/GIN with no token stats)
///   rows_newest      : EXACT total rows of the first-read selected partition, summed worker-side
///                      from its parts' full rows_count.
///   fulltext_default : decision to use when selectivity is unknown
///   safety           : multiplier on LIMIT; >1 makes the gate more conservative
///
inline bool partitionOrderGate(
    size_t limit,
    double selectivity,
    UInt64 rows_newest,
    bool fulltext_default,
    double safety)
{
    /// No LIMIT -> no early-termination opportunity -> partition-order can only lose.
    if (limit == 0)
        return false;

    /// Unknown selectivity: defer to the full-text policy.
    if (selectivity < 0.0)
        return fulltext_default;

    if (rows_newest == 0)
        return false;

    /// Matching rows in the newest partition = selectivity applied to its exact row count.
    const double est_newest_rows = selectivity * static_cast<double>(rows_newest);
    return est_newest_rows >= static_cast<double>(limit) * safety;
}

}
