package utils

import com.example.bankanalysis.transformation.{Aggregations, WindowFunctions}
import org.apache.spark.sql.DataFrame

object TransformationsConfiguration {
  val methods: Map[String, DataFrame => DataFrame] = Map(
    "top10TotalTransactionAmountByCustomer" -> Aggregations.top10TotalTransactionAmountByCustomer,
    "monthlyTransactionVolumeByBranch" -> Aggregations.monthlyTransactionVolumeByBranch,
    "weeklyAverageTransactionAmountByCustomer" -> WindowFunctions.weeklyAverageTransactionAmountByCustomer,
    "customerRankByBranchOnTransactionAmount" -> WindowFunctions.customerRankByBranchOnTransactionAmount
  )
}
