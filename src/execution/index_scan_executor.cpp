//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  auto *tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  index_iter_ = tree_index->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!index_iter_.IsEnd()) {
    auto tmp_rid = (*index_iter_).second;
    ++index_iter_;
    auto result = table_info_->table_->GetTuple(tmp_rid);
    if (result.first.is_deleted_) {
      continue;
    }
    *tuple = result.second;
    *rid = tmp_rid;
    return true;
  }
  return false;
}

}  // namespace bustub
