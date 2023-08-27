#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      pq_(TopNCompare(plan_->GetOrderBy(), plan_->OutputSchema())) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    pq_.emplace(tuple);
    if (pq_.size() > plan_->GetN()) {
      pq_.pop();
    }
  }
  while (!pq_.empty()) {
    tuples_.emplace_back(pq_.top());
    pq_.pop();
  }
  iter_ = tuples_.crbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.crend()) {
    return false;
  }
  *tuple = *iter_;
  ++iter_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return pq_.size(); };

}  // namespace bustub
