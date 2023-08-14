//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple old_tuple;
  RID old_rid;
  int32_t cnt = 0;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // delete tuple
    auto tuple_meta = table_info_->table_->GetTupleMeta(old_rid);
    BUSTUB_ASSERT(!tuple_meta.is_deleted_, "update executor should not receive any deleted tuple");
    for (IndexInfo *index_info : index_infos_) {
      Tuple key_tuple =
          old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, old_rid, exec_ctx_->GetTransaction());
    }
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, old_rid);

    // manufacture new tuple
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.emplace_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(new_values, &child_executor_->GetOutputSchema());

    // insert new tuple
    auto new_rid = table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple,
                                                    exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction());
    if (!new_rid.has_value()) {
      continue;
    }
    for (const IndexInfo *index_info : index_infos_) {
      const Tuple &key_tuple =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, new_rid.value(), exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  *tuple = Tuple{{{INTEGER, cnt}}, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
