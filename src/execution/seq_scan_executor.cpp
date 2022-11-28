//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_.get();
  table_iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (table_iter_ != table_heap_->End()) {
    *tuple = *table_iter_;
    ++table_iter_;
    *rid = tuple->GetRid();
    if ( plan_->filter_predicate_ == nullptr
        || plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
      return true;
    }
  }
  return false;
}
}  // namespace bustub
