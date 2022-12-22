//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  RID child_rid{};
  int32_t cnt = 0;
  while(child_executor_->Next(&child_tuple, &child_rid)) {
    if(DeleteTuple(&child_tuple, child_rid)) {
      cnt++;
    }
  }
  *tuple = Tuple{{{INTEGER, cnt}}, &plan_->OutputSchema()};
  return cnt>0;
}

auto DeleteExecutor::DeleteTuple(Tuple *tuple, RID rid) -> bool {
  if(!table_info_->table_->MarkDelete(rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  for(IndexInfo *index_info : index_infos_) {
    Tuple key_tuple = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->DeleteEntry(key_tuple, rid, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
