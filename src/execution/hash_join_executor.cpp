//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/util/hash_util.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  ht_.clear();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    const auto &right_key = GetRightJoinKey(&tuple);
    auto iter = ht_.find(right_key);
    if (iter == ht_.end()) {
      ht_.emplace(right_key, std::vector<Tuple>{});
      iter = ht_.find(right_key);
    }
    iter->second.emplace_back(tuple);
  }
  left_executor_->Init();
  GetNextLeftTuple();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (left_end_) {
    return false;
  }
  bool find_right = false;
  while (!find_right && !left_end_) {
    if (right_end_opt_.has_value()) {
      auto right_end = right_end_opt_.value();
      if (right_iter_ != right_end) {
        find_right = true;
        last_left_match_ = true;
        auto right_tuple = *right_iter_;
        ++right_iter_;
        std::vector<Value> vals;
        vals.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                     right_executor_->GetOutputSchema().GetColumnCount());
        for (auto i = 0U; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          vals.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (auto i = 0U; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          vals.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple{vals, &GetOutputSchema()};
      }
    }
    if (!find_right && !last_left_match_ && plan_->GetJoinType() == JoinType::LEFT) {
      find_right = true;
      std::vector<Value> vals;
      vals.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                   right_executor_->GetOutputSchema().GetColumnCount());
      for (auto i = 0U; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        vals.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (auto i = 0U; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        vals.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple{vals, &GetOutputSchema()};
    }
    if (!right_end_opt_.has_value() || right_iter_ == right_end_opt_.value()) {
      GetNextLeftTuple();
    }
  }
  return find_right;
}

auto HashJoinExecutor::GetLeftJoinKey(const Tuple *tuple) -> JoinKey {
  return JoinKey{GetJoinKeys(tuple, left_executor_->GetOutputSchema(), plan_->LeftJoinKeyExpressions())};
}

auto HashJoinExecutor::GetRightJoinKey(const Tuple *tuple) -> JoinKey {
  return JoinKey{GetJoinKeys(tuple, right_executor_->GetOutputSchema(), plan_->RightJoinKeyExpressions())};
}

auto HashJoinExecutor::GetJoinKeys(const Tuple *tuple, const Schema &schema,
                                   const std::vector<AbstractExpressionRef> &expressions) const -> std::vector<Value> {
  std::vector<Value> join_keys;
  join_keys.reserve(expressions.size());
  for (auto &expr : expressions) {
    join_keys.emplace_back(expr->Evaluate(tuple, schema));
  }
  return join_keys;
}

void HashJoinExecutor::GetNextLeftTuple() {
  last_left_match_ = false;
  right_end_opt_.reset();
  RID left_rid;
  left_end_ = !left_executor_->Next(&left_tuple_, &left_rid);
  if (!left_end_) {
    if (auto iter = ht_.find(GetLeftJoinKey(&left_tuple_)); iter != ht_.end()) {
      right_iter_ = iter->second.begin();
      right_end_opt_ = iter->second.end();
    }
  }
}

}  // namespace bustub
