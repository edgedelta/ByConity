/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <common/types.h>
#include <Core/Defines.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <type_traits>
#include <common/logger_useful.h>

#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <llvm/IR/IRBuilder.h>
#pragma GCC diagnostic pop
#endif


/** Logical functions AND, OR, XOR and NOT support three-valued (or ternary) logic
  * https://en.wikibooks.org/wiki/Structured_Query_Language/NULLs_and_the_Three_Valued_Logic
  *
  * Functions XOR and NOT rely on "default implementation for NULLs":
  *   - if any of the arguments is of Nullable type, the return value type is Nullable
  *   - if any of the arguments is NULL, the return value is NULL
  *
  * Functions AND and OR provide their own special implementations for ternary logic
  */

namespace DB
{

struct NameAnd { static constexpr auto name = "and"; };
struct NameOr { static constexpr auto name = "or"; };
struct NameXor { static constexpr auto name = "xor"; };
struct NameNot { static constexpr auto name = "not"; };

namespace FunctionsLogicalDetail
{
namespace Ternary
{
    using ResultType = UInt8;

    /** These carefully picked values magically work so bitwise "and", "or" on them
      *  corresponds to the expected results in three-valued logic.
      *
      * False and True are represented by all-0 and all-1 bits, so all bitwise operations on them work as expected.
      * Null is represented as single 1 bit. So, it is something in between False and True.
      * And "or" works like maximum and "and" works like minimum:
      *  "or" keeps True as is and lifts False with Null to Null.
      *  "and" keeps False as is and downs True with Null to Null.
      *
      * This logic does not apply for "not" and "xor" - they work with default implementation for NULLs:
      *  anything with NULL returns NULL, otherwise use conventional two-valued logic.
      */
    static constexpr UInt8 False = 0;   /// All zero bits.
    static constexpr UInt8 True = -1;   /// All one bits.
    static constexpr UInt8 Null = 1;    /// Single one bit.

    template <typename T>
    inline ResultType makeValue(T value)
    {
        return value != 0 ? Ternary::True : Ternary::False;
    }

    template <typename T>
    inline ResultType makeValue(T value, bool is_null)
    {
        if (is_null)
            return Ternary::Null;
        return makeValue<T>(value);
    }
}


struct AndImpl
{
    using ResultType = UInt8;

    static inline constexpr bool isSaturable() { return true; }

    /// Final value in two-valued logic (no further operations with True, False will change this value)
    static inline constexpr bool isSaturatedValue(bool a) { return !a; }

    /// Final value in three-valued logic (no further operations with True, False, Null will change this value)
    static inline constexpr bool isSaturatedValueTernary(UInt8 a) { return a == Ternary::False; }

    static inline constexpr bool isNeutralValueTernary(UInt8 a) { return a == Ternary::True; }

    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a & b; }

    /// Will use three-valued logic for NULLs (see above) or default implementation (any operation with NULL returns NULL).
    static inline constexpr bool specialImplementationForNulls() { return true; }

    static inline constexpr bool isCompilable() { return true; }
};

struct OrImpl
{
    using ResultType = UInt8;

    static inline constexpr bool isSaturable() { return true; }
    static inline constexpr bool isSaturatedValue(bool a) { return a; }
    static inline constexpr bool isSaturatedValueTernary(UInt8 a) { return a == Ternary::True; }
    static inline constexpr bool isNeutralValueTernary(UInt8 a) { return a == Ternary::False; }
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a | b; }
    static inline constexpr bool specialImplementationForNulls() { return true; }
    static inline constexpr bool isCompilable() { return true; }
};

struct XorImpl
{
    using ResultType = UInt8;

    static inline constexpr bool isSaturable() { return false; }
    static inline constexpr bool isSaturatedValue(bool) { return false; }
    static inline constexpr bool isSaturatedValueTernary(UInt8) { return false; }
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a != b; }
    static inline constexpr bool specialImplementationForNulls() { return false; }
    static inline constexpr bool isCompilable() { return true; }

#if USE_EMBEDDED_COMPILER
    static inline llvm::Value * apply(llvm::IRBuilder<> & builder, llvm::Value * a, llvm::Value * b)
    {
        return builder.CreateXor(a, b);
    }
#endif
};

template <typename A>
struct NotImpl
{
    using ResultType = UInt8;

    static inline ResultType apply(A a)
    {
        return !a;
    }

#if USE_EMBEDDED_COMPILER
    static inline llvm::Value * apply(llvm::IRBuilder<> & builder, llvm::Value * a)
    {
        return builder.CreateNot(a);
    }
#endif
};

template <typename Impl, typename Name>
class FunctionAnyArityLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionAnyArityLogical>(); }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isShortCircuit(ShortCircuitSettings & settings, size_t /*number_of_arguments*/) const override
    {
        settings.enable_lazy_execution_for_first_argument = false;
        settings.enable_lazy_execution_for_common_descendants_of_arguments = true;
        settings.force_enable_lazy_execution = false;
        return name == NameAnd::name || name == NameOr::name;
    }
    ColumnPtr executeShortCircuit(ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return !Impl::specialImplementationForNulls(); }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const override;

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override;

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes &) const override { return Impl::isCompilable(); }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, Values values, JITContext & ) const override
    {
        assert(!types.empty() && !values.empty());

        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        if constexpr (!Impl::isSaturable())
        {
            auto * result = nativeBoolCast(b, types[0], values[0]);
            for (size_t i = 1; i < types.size(); i++)
                result = Impl::apply(b, result, nativeBoolCast(b, types[i], values[i]));
            return b.CreateSelect(result, b.getInt8(1), b.getInt8(0));
        }
        constexpr bool breakOnTrue = Impl::isSaturatedValue(true);
        /// check function args has nullable columns
        bool has_nullable = false;
        for (const auto & type : types)
        {
            has_nullable = type->isNullable();
            if (has_nullable)
            {
                break;
            }
        }
        auto * next = b.GetInsertBlock();
        auto * stop = llvm::BasicBlock::Create(next->getContext(), "", next->getParent());
        b.SetInsertPoint(stop);
        auto * phi = has_nullable ? b.CreatePHI(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}), values.size()) : b.CreatePHI(b.getInt8Ty(), values.size());
        llvm::Value * has_null = nullptr;
        for (size_t i = 0; i < types.size(); i++)
        {
            b.SetInsertPoint(next);
            auto * value = values[i];
            /// handle null value
            llvm::Value * truth = nullptr;
            if (types[i]->isNullable())
            {
                /// get data and null flag
                auto * data = nativeBoolCast(b, removeNullable(types[i]), b.CreateExtractValue(value, {0}));
                auto * is_null = b.CreateExtractValue(value, {1});
                truth = breakOnTrue ? b.CreateAnd(data, b.CreateNot(is_null)) : b.CreateOr(data, is_null);
                data = b.CreateSelect(truth, b.getInt8(1), b.getInt8(0));

                /// add a null flag for or function
                if (has_null == nullptr)
                    has_null = b.CreateICmpNE(is_null, b.getInt1(false));
                else
                    has_null = b.CreateOr(has_null, b.CreateICmpNE(is_null, b.getInt1(false)));

                if (i + 1 == types.size() && has_null != nullptr && has_nullable)
                {
                    /// handle or
                    if (breakOnTrue)
                    {
                        auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                        auto * tmp_is_null = b.CreateSelect(has_null, b.getInt1(true), b.getInt1(false));
                        auto * nullable_value_1 = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, b.getInt8(0), {0}), tmp_is_null, {1});
                        nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                        auto * nullable_value_2 = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, data, {0}), is_null, {1});
                        /// if current branch is true, return true, else return null or false
                        auto * final_nullable_vale = b.CreateSelect(truth, nullable_value_2, nullable_value_1);
                        phi->addIncoming(final_nullable_vale, b.GetInsertBlock());
                    }
                    /// handle and
                    else
                    {
                        auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                        auto * tmp_is_null = b.CreateSelect(has_null, b.getInt1(true), b.getInt1(false));
                        auto * tmp_value = b.CreateSelect(has_null, b.getInt8(0), b.getInt8(1));
                        auto * nullable_value_1 = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, tmp_value, {0}), tmp_is_null, {1});
                        nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                        auto * nullable_value_2 = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, data, {0}), is_null, {1});
                        /// if current branch is false, return false, else return null or true
                        auto * final_nullable_vale = b.CreateSelect(b.CreateNot(truth), nullable_value_2, nullable_value_1);
                        phi->addIncoming(final_nullable_vale, b.GetInsertBlock());
                    }
                }
                else
                {
                    auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                    auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, data, {0}), is_null, {1});
                    phi->addIncoming(nullable_value, b.GetInsertBlock());
                }
            }
            else
            {
                truth = nativeBoolCast(b, types[i], value);
                if (!types[i]->equals(DataTypeUInt8{}))
                    value = b.CreateSelect(truth, b.getInt8(1), b.getInt8(0));
                if (i + 1 == types.size() && has_null != nullptr && has_nullable)
                {
                    /// handle or
                    if (breakOnTrue)
                    {
                        auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                        auto * tmp_is_null = b.CreateSelect(has_null, b.getInt1(true), b.getInt1(false));
                        auto * nullable_value_1 = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, b.getInt8(0), {0}), tmp_is_null, {1});
                        nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                        auto * nullable_value_2 = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), b.getInt1(false), {1});
                        /// if current branch is true, return true, else return null or false
                        auto * final_nullable_vale = b.CreateSelect(truth, nullable_value_2, nullable_value_1);
                        phi->addIncoming(final_nullable_vale, b.GetInsertBlock());
                    }
                    /// handle and
                    else
                    {
                        auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                        auto * tmp_is_null = b.CreateSelect(has_null, b.getInt1(true), b.getInt1(false));
                        auto * tmp_value = b.CreateSelect(has_null, b.getInt8(0), b.getInt8(1));
                        auto * nullable_value_1 = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, tmp_value, {0}), tmp_is_null, {1});
                        nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                        auto * nullable_value_2 = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), b.getInt1(false), {1});
                        /// if current branch is false, return false, else return null or true
                        auto * final_nullable_vale = b.CreateSelect(b.CreateNot(truth), nullable_value_2, nullable_value_1);
                        phi->addIncoming(final_nullable_vale, b.GetInsertBlock());
                    }
                }
                else
                {
                    if (has_nullable)
                    {
                        auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, DataTypeNullable{std::make_shared<DataTypeUInt8>()}));
                        auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), b.getInt1(false), {1});
                        phi->addIncoming(nullable_value, b.GetInsertBlock());
                    }
                    else
                    {
                        phi->addIncoming(value, b.GetInsertBlock());
                    }
                }
            }
            if (i + 1 < types.size())
            {
                next = llvm::BasicBlock::Create(next->getContext(), "", next->getParent());
                b.CreateCondBr(truth, breakOnTrue ? stop : next, breakOnTrue ? next : stop);
            }
        }
        b.CreateBr(stop);
        b.SetInsertPoint(stop);
        return phi;
    }
#endif
};


template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUnaryLogical>(); }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override;

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes &) const override { return true; }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, Values values, JITContext & ) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        return b.CreateSelect(Impl<UInt8>::apply(b, nativeBoolCast(b, types[0], values[0])), b.getInt8(1), b.getInt8(0));
    }
#endif
};

}

using FunctionAnd = FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::AndImpl, NameAnd>;
using FunctionOr = FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::OrImpl, NameOr>;
using FunctionXor = FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::XorImpl, NameXor>;
using FunctionNot = FunctionsLogicalDetail::FunctionUnaryLogical<FunctionsLogicalDetail::NotImpl, NameNot>;

}
