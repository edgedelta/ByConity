#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include "Columns/ColumnsCommon.h"
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Core/ColumnWithTypeAndName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}


ConstantFilterDescription::ConstantFilterDescription(const IColumn & column)
{
    if (column.onlyNull())
    {
        always_false = true;
        return;
    }

    if (isColumnConst(column))
    {
        const ColumnConst & column_const = assert_cast<const ColumnConst &>(column);
        ColumnPtr column_nested = column_const.getDataColumnPtr()->convertToFullColumnIfLowCardinality();

        if (!typeid_cast<const ColumnUInt8 *>(column_nested.get()))
        {
            const ColumnNullable * column_nested_nullable = checkAndGetColumn<ColumnNullable>(*column_nested);
            if (!column_nested_nullable || !typeid_cast<const ColumnUInt8 *>(&column_nested_nullable->getNestedColumn()))
            {
                throw Exception("Illegal type " + column_nested->getName() + " of column for constant filter. Must be UInt8 or Nullable(UInt8).",
                                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
            }
        }

        if (column_const.getValue<UInt64>())
            always_true = true;
        else
            always_false = true;
        return;
    }
}


FilterDescription::FilterDescription(const IColumn & column_)
{
    if (column_.lowCardinality())
        data_holder = column_.convertToFullColumnIfLowCardinality();

    const auto & column = data_holder ? *data_holder : column_;

    if (const ColumnUInt8 * concrete_column = typeid_cast<const ColumnUInt8 *>(&column))
    {
        data = &concrete_column->getData();
        return;
    }

    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(column))
    {
        ColumnPtr nested_column = nullable_column->getNestedColumnPtr();
        MutableColumnPtr mutable_holder = IColumn::mutate(std::move(nested_column));

        ColumnUInt8 * concrete_column = typeid_cast<ColumnUInt8 *>(mutable_holder.get());
        if (!concrete_column)
            throw Exception("Illegal type " + column.getName() + " of column for filter. Must be UInt8 or Nullable(UInt8).",
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

        const NullMap & null_map = nullable_column->getNullMapData();
        IColumn::Filter & res = concrete_column->getData();

        size_t size = res.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = res[i] && !null_map[i];

        data = &res;
        data_holder = std::move(mutable_holder);
        return;
    }

    throw Exception("Illegal type " + column.getName() + " of column for filter. Must be UInt8 or Nullable(UInt8) or Const variants of them.",
        ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
}

bool FilterDescription::hasOnes() const
{
    if (has_one >= 0)
        return has_one;

    return data ? (has_one = !memoryIsZero(data->data(), 0, data->size())) : false;
}

ConstantFilterDescription merge(const ConstantFilterDescription & lhs, const ConstantFilterDescription & rhs)
{
    ConstantFilterDescription res;
    res.always_false = lhs.always_false || rhs.always_false;
    res.always_true = lhs.always_true && rhs.always_true;
    return res;
}

}

