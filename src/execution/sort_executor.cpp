#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  std::sort(
      tuples_.begin(), tuples_.end(),
      [&order_by = plan_->GetOrderBy(), &schema = GetOutputSchema()](const Tuple &left, const Tuple &right) {
        for (const auto &[order_by_type, expr] : order_by) {
          if (expr->Evaluate(&left, schema).CompareLessThan(expr->Evaluate(&right, schema)) == CmpBool::CmpTrue) {
            return order_by_type != OrderByType::DESC;
          }
          if (expr->Evaluate(&left, schema).CompareGreaterThan(expr->Evaluate(&right, schema)) == CmpBool::CmpTrue) {
            return order_by_type == OrderByType::DESC;
          }
        }
        return false;
      });
  iter_ = tuples_.cbegin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.cend()) {
    return false;
  }
  *tuple = *iter_;
  ++iter_;
  return true;
}

}  // namespace bustub
