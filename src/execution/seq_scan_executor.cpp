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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  iter_ = std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    auto [tuple_meta, cur_tuple] = iter_->GetTuple();
    ++(*iter_);
    if (!tuple_meta.is_deleted_ &&
        (plan_->filter_predicate_ == nullptr ||
         plan_->filter_predicate_->Evaluate(&cur_tuple, plan_->OutputSchema()).GetAs<bool>())) {
      *tuple = cur_tuple;
      *rid = cur_tuple.GetRid();
      return true;
    }
  }
  return false;
}
}  // namespace bustub
