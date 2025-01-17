//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple child_tuple{};
  RID child_rid{};
  int32_t cnt = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    if (InsertTupleAndIndices(child_tuple, exec_ctx_->GetTransaction())) {
      cnt++;
    }
  }
  *tuple = Tuple{{{INTEGER, cnt}}, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

auto InsertExecutor::InsertTupleAndIndices(Tuple &tuple, Transaction *txn) -> bool {
  auto rid = table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, tuple,
                                              exec_ctx_->GetLockManager(), txn);
  if (!rid.has_value()) {
    return false;
  }

  for (const IndexInfo *index_info : index_infos_) {
    const Tuple &key_tuple =
        tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->InsertEntry(key_tuple, rid.value(), txn);
  }
  return true;
}

}  // namespace bustub
