package co.ledger.wallet.daemon.controllers

import co.ledger.core.{BitcoinLikePickingStrategy, RippleLikeMemo}
import co.ledger.wallet.daemon.async.MDCPropagatingExecutionContext
import co.ledger.wallet.daemon.controllers.requests.{CommonMethodValidations, RequestWithUser}
import co.ledger.wallet.daemon.models.{AccountInfo, FeeMethod}
import co.ledger.wallet.daemon.services.TransactionsService
import co.ledger.wallet.daemon.utils.HexUtils
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import com.twitter.finatra.request.RouteParam
import com.twitter.finatra.validation.{MethodValidation, ValidationResult}
import javax.inject.Inject

import scala.concurrent.ExecutionContext

class TransactionsController @Inject()(transactionsService: TransactionsService) extends Controller {
  implicit val ec: ExecutionContext = MDCPropagatingExecutionContext.Implicits.global

  import TransactionsController._

  /**
    * Transaction creation method.
    * Input json
    * {
    * recipient: recipient address,
    * fees_per_byte: optional(in satoshi),
    * fees_level: optional(SLOW, FAST, NORMAL),
    * amount: in satoshi,
    * exclude_utxos: map{txHash: index}
    * }
    *
    */
  post("/pools/:pool_name/wallets/:wallet_name/accounts/:account_index/transactions") { request: AccountInfoRequest =>
    info(s"Create transaction $request: ${request.request.contentString}")
    transactionsService.createTransaction(request.request, request.accountInfo)
  }

  /**
    * Send a signed transaction.
    * Input json
    * {
    * raw_transaction: the bytes,
    * signatures: [string],
    * pubkeys: [string]
    * }
    */
  post("/pools/:pool_name/wallets/:wallet_name/accounts/:account_index/transactions/sign") { request: AccountInfoRequest =>
    info(s"Sign transaction $request: ${request.request.contentString}")
    transactionsService.broadcastTransaction(request.request, request.accountInfo)
  }

}

object TransactionsController {

  case class AccountInfoRequest(@RouteParam pool_name: String,
                                @RouteParam wallet_name: String,
                                @RouteParam account_index: Int,
                                request: Request
                               ) extends RequestWithUser
  {
    def accountInfo: AccountInfo = AccountInfo(account_index, wallet_name, pool_name, user.pubKey)
  }

  trait BroadcastTransactionRequest

  case class BroadcastETHTransactionRequest(raw_transaction: String,
                                            signatures: Seq[String],
                                            request: Request
                                           ) extends BroadcastTransactionRequest {
    def hexTx: Array[Byte] = HexUtils.valueOf(raw_transaction)
    def hexSig: Array[Byte] = HexUtils.valueOf(signatures.head)

    @MethodValidation
    def validateSignatures: ValidationResult = ValidationResult.validate(
      signatures.size == 1,
      s"expecting 1 DER signature, found ${signatures.size} instead."
    )
  }

  case class BroadcastBTCTransactionRequest(raw_transaction: String,
                                            signatures: Seq[String],
                                            pubkeys: Seq[String],
                                            request: Request
                                           ) extends BroadcastTransactionRequest {
    def rawTx: Array[Byte] = HexUtils.valueOf(raw_transaction)
    def pairedSignatures: Seq[(Array[Byte], Array[Byte])] = signatures.zipWithIndex.map { case (sig, index) =>
      (HexUtils.valueOf(sig), HexUtils.valueOf(pubkeys(index)))
    }

    @MethodValidation
    def validateSignatures: ValidationResult = ValidationResult.validate(
      signatures.size == pubkeys.size,
      "signatures and pubkeys size not matching")
  }

  trait CreateTransactionRequest {
    def transactionInfo: TransactionInfo
  }

  case class BroadcastXRPTransactionRequest(raw_transaction: String,
                                            signatures: Seq[String],
                                            request: Request
                                           ) extends BroadcastTransactionRequest {
    def hexTx: Array[Byte] = HexUtils.valueOf(raw_transaction)
    def hexSig: Array[Byte] = HexUtils.valueOf(signatures.head)

    @MethodValidation
    def validateSignatures: ValidationResult = ValidationResult.validate(
      signatures.size == 1,
      s"expecting 1 DER signature, found ${signatures.size} instead."
    )
  }

  case class CreateETHTransactionRequest(recipient: String,
                                         amount: String,
                                         gas_limit: Option[String],
                                         gas_price: Option[String],
                                         contract: Option[String]
                                        ) extends CreateTransactionRequest {
    def amountValue: BigInt = BigInt(amount)
    def gasLimitValue: Option[BigInt] = gas_limit.map(BigInt(_))
    def gasPriceValue: Option[BigInt] = gas_price.map(BigInt(_))

    override def transactionInfo: TransactionInfo = ETHTransactionInfo(recipient, amountValue, gasLimitValue, gasPriceValue, contract)
  }

  case class CreateBTCTransactionRequest(recipient: String,
                                         fees_per_byte: Option[String],
                                         fees_level: Option[String],
                                         amount: String,
                                         picking_strategy: Option[String],
                                         exclude_utxos: Option[Map[String, Int]]
                                        ) extends CreateTransactionRequest {
    def amountValue: BigInt = BigInt(amount)
    def feesPerByteValue: Option[BigInt] = fees_per_byte.map(BigInt(_))

    def transactionInfo: BTCTransactionInfo = BTCTransactionInfo(recipient, feesPerByteValue, fees_level, amountValue, picking_strategy, exclude_utxos.getOrElse(Map[String, Int]()))

    @MethodValidation
    def validateFees: ValidationResult = CommonMethodValidations.validateFees(feesPerByteValue, fees_level)
  }

  case class XRPSendTo(amount: BigInt, address: String)

  case class XRPSendToRequest(amount: String, address: String)

  case class CreateXRPTransactionRequest(send_to: List[XRPSendToRequest], // Send funds to the given address.
                                         wipe_to: Option[String],         // Send all available funds to the given address.
                                         fees: Option[String],            // Fees (in drop) the originator is willing to pay
                                         memos: List[RippleLikeMemo],     // Memos to add for this transaction
                                         destination_tag: Option[Long]    // An arbitrary unsigned 32-bit integer that identifies a reason for payment or a non-Ripple account
                                        ) extends CreateTransactionRequest {
    def sendToValue: List[XRPSendTo] = send_to.map { r => XRPSendTo(BigInt(r.amount), r.address) }
    def feesValue: Option[BigInt] = fees.map(BigInt(_))

    override def transactionInfo: TransactionInfo = XRPTransactionInfo(sendToValue, wipe_to, feesValue: Option[BigInt], memos, destination_tag)
  }

  trait TransactionInfo

  case class BTCTransactionInfo(recipient: String, feeAmount: Option[BigInt], feeLevel: Option[String], amount: BigInt, UTXOPickingStrategy: Option[String], excludeUtxos: Map[String, Int]) extends TransactionInfo {
    lazy val feeMethod: Option[FeeMethod] = feeLevel.map { level => FeeMethod.from(level) }
    lazy val pickingStrategy: Option[BitcoinLikePickingStrategy] = UTXOPickingStrategy.map {
        case "DEEP_OUTPUTS_FIRST" => BitcoinLikePickingStrategy.DEEP_OUTPUTS_FIRST
        case "OPTIMIZE_SIZE" => BitcoinLikePickingStrategy.OPTIMIZE_SIZE
        case "MERGE_OUTPUTS" => BitcoinLikePickingStrategy.MERGE_OUTPUTS
        case others => throw new Exception(s"Unknown picking strategy $others")
    }
  }

  case class ETHTransactionInfo(recipient: String, amount: BigInt, gasLimit: Option[BigInt], gasPrice: Option[BigInt], contract: Option[String]) extends TransactionInfo

  case class XRPTransactionInfo(sendTo: List[XRPSendTo], wipeTo: Option[String], fees: Option[BigInt], memos: List[RippleLikeMemo], destinationTag: Option[Long]) extends TransactionInfo
}
