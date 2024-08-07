#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/castColumn.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionArrayPush : public IFunction
{
public:
    FunctionArrayPush(bool push_front_, const char * name_)
        : push_front(push_front_), name(name_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[0];

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(arguments[0].get());
        if (nullable_type)
            array_type = checkAndGetDataType<DataTypeArray>(nullable_type->getNestedType().get());
        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array but it has type "
                            + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto nested_type = array_type->getNestedType();

        DataTypes types = {nested_type, arguments[1]};

        if (nullable_type)
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(getLeastSupertype(types)));

        return std::make_shared<DataTypeArray>(getLeastSupertype(types));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Execute on nested columns and wrap results with nullable
        auto * nullable_col = checkAndGetColumn<ColumnNullable>(arguments[0].column.get());

        if (nullable_col)
        {
            ColumnsWithTypeAndName tmp_args = {{nullable_col->getNestedColumnPtr(), removeNullable(arguments[0].type), arguments[0].name}, arguments[1]};
            auto res = executeInternalImpl(tmp_args, removeNullable(result_type), input_rows_count);
            return wrapInNullable(res, arguments, result_type, input_rows_count);
        }

        return executeInternalImpl(arguments, result_type, input_rows_count);
    }

    ColumnPtr executeInternalImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const
    {
        if (return_type->onlyNull())
            return return_type->createColumnConstWithDefaultValue(input_rows_count);

        auto result_column = return_type->createColumn();

        auto array_column = arguments[0].column;
        auto appended_column = arguments[1].column;

        if (!arguments[0].type->equals(*return_type))
            array_column = castColumn(arguments[0], return_type);

        const DataTypePtr & return_nested_type = typeid_cast<const DataTypeArray &>(*return_type).getNestedType();
        if (!arguments[1].type->equals(*return_nested_type))
            appended_column = castColumn(arguments[1], return_nested_type);

        std::unique_ptr<GatherUtils::IArraySource> array_source;
        std::unique_ptr<GatherUtils::IValueSource> value_source;

        size_t size = array_column->size();
        bool is_const = false;

        if (const auto * const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
        {
            is_const = true;
            array_column = const_array_column->getDataColumnPtr();
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
            array_source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
        else
            throw Exception{"First arguments for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};


        bool is_appended_const = false;
        if (const auto * const_appended_column = typeid_cast<const ColumnConst *>(appended_column.get()))
        {
            is_appended_const = true;
            appended_column = const_appended_column->getDataColumnPtr();
        }

        value_source = GatherUtils::createValueSource(*appended_column, is_appended_const, size);

        auto sink = GatherUtils::createArraySink(typeid_cast<ColumnArray &>(*result_column), size);

        GatherUtils::push(*array_source, *value_source, *sink, push_front);

        return result_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

private:
    bool push_front;
    const char * name;
};

}
