//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  RightExecutorInit();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (left_end_) {
    return false;
  }
  Tuple right_tuple;
  RID right_id;
  bool find_right = false;
  while (!find_right && !left_end_) {
    while (right_executor_->Next(&right_tuple, &right_id)) {
      auto equal = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                    right_executor_->GetOutputSchema());
      if (equal.GetAs<bool>()) {
        find_right = true;
        last_left_match_ = true;
        std::vector<Value> vals;
        auto left_col_num = left_executor_->GetOutputSchema().GetColumnCount();
        auto right_col_num = right_executor_->GetOutputSchema().GetColumnCount();
        vals.reserve(left_col_num + right_col_num);
        for (auto i = 0U; i < left_col_num; i++) {
          vals.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (auto i = 0U; i < right_col_num; i++) {
          vals.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(vals, &GetOutputSchema());
        return find_right;
      }
    }

    if (plan_->GetJoinType() == JoinType::LEFT && !last_left_match_) {
      find_right = true;
      std::vector<Value> vals;
      auto left_col_num = left_executor_->GetOutputSchema().GetColumnCount();
      auto right_col_num = right_executor_->GetOutputSchema().GetColumnCount();
      vals.reserve(left_col_num + right_col_num);
      for (auto i = 0U; i < left_col_num; i++) {
        vals.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (auto i = 0U; i < right_col_num; i++) {
        vals.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(vals, &GetOutputSchema());
    }
    RightExecutorInit();
  }
  return find_right;
}

void NestedLoopJoinExecutor::RightExecutorInit() {
  last_left_match_ = false;
  RID left_rid;
  left_end_ = !left_executor_->Next(&left_tuple_, &left_rid);
  if (!left_end_) {
    right_executor_->Init();
  }
}

}  // namespace bustub
