//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  struct TopNCompare {
    TopNCompare(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by, const Schema &schema)
        : order_by_(order_by), schema_(schema) {}
    auto operator()(const Tuple &left, const Tuple &right) -> bool {
      for (const auto &[order_by_type, expr] : order_by_) {
        if (expr->Evaluate(&left, schema_).CompareLessThan(expr->Evaluate(&right, schema_)) == CmpBool::CmpTrue) {
          return order_by_type != OrderByType::DESC;
        }
        if (expr->Evaluate(&left, schema_).CompareGreaterThan(expr->Evaluate(&right, schema_)) == CmpBool::CmpTrue) {
          return order_by_type == OrderByType::DESC;
        }
      }
      return false;
    }

    const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by_;
    const Schema &schema_;
  };
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::priority_queue<Tuple, std::vector<Tuple>, TopNCompare> pq_;
  std::vector<Tuple> tuples_;
  std::vector<Tuple>::const_reverse_iterator iter_;
};
}  // namespace bustub
