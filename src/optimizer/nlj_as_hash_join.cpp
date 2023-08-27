#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  //
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }
  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  // Has exactly two children
  BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

  // Check if expr is equal condition where one is for the left table, and one is for the right table.
  if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
    auto res = ExtractColExprForColEqualComparison(expr);
    if (res.has_value()) {
      return std::make_shared<HashJoinPlanNode>(
          nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
          std::vector<AbstractExpressionRef>{std::move(res->first)},
          std::vector<AbstractExpressionRef>{std::move(res->second)}, nlj_plan.GetJoinType());
    }
  }

  if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
      expr != nullptr && expr->logic_type_ == LogicType::And) {
    const auto *left_expr = dynamic_cast<const ComparisonExpression *>(expr->GetChildAt(0).get());
    const auto *right_expr = dynamic_cast<const ComparisonExpression *>(expr->GetChildAt(1).get());
    if (left_expr != nullptr && right_expr != nullptr) {
      if (auto left_res = ExtractColExprForColEqualComparison(left_expr); left_res.has_value()) {
        if (auto right_res = ExtractColExprForColEqualComparison(right_expr); right_res.has_value()) {
          return std::make_shared<HashJoinPlanNode>(
              nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
              std::vector<AbstractExpressionRef>{std::move(left_res->first), std::move(right_res->first)},
              std::vector<AbstractExpressionRef>{std::move(left_res->second), std::move(right_res->second)},
              nlj_plan.GetJoinType());
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::ExtractColExprForColEqualComparison(const ComparisonExpression *expr)
    -> std::optional<std::pair<std::shared_ptr<ColumnValueExpression>, std::shared_ptr<ColumnValueExpression>>> {
  if (expr->comp_type_ != ComparisonType::Equal) {
    return std::nullopt;
  }
  if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
      left_expr != nullptr) {
    if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
        right_expr != nullptr) {
      // Ensure both exprs have tuple_id == 0
      auto left_expr_tuple =
          std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
      auto right_expr_tuple =
          std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
      if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
        return std::make_pair(std::move(left_expr_tuple), std::move(right_expr_tuple));
      }
      if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
        return std::make_pair(std::move(right_expr_tuple), std::move(left_expr_tuple));
      }
    }
  }
  return std::nullopt;
}

}  // namespace bustub
