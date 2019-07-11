package co.ledger.wallet.daemon.models

import java.util.{Calendar, Date}

import cats.implicits._
import co.ledger.core
import co.ledger.core._
import co.ledger.core.implicits.{UnsupportedOperationException, _}
import co.ledger.wallet.daemon.clients.ClientFactory
import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import co.ledger.wallet.daemon.controllers.TransactionsController.{BTCTransactionInfo, ETHTransactionInfo, TransactionInfo}
import co.ledger.wallet.daemon.exceptions.{ERC20BalanceNotEnough, ERC20NotFoundException, SignatureSizeUnmatchException}
import co.ledger.wallet.daemon.libledger_core.async.LedgerCoreExecutionContext
import co.ledger.wallet.daemon.models.Currency._
import co.ledger.wallet.daemon.models.coins.Coin.TransactionView
import co.ledger.wallet.daemon.models.coins.{Bitcoin, UnsignedEthereumTransactionView}
import co.ledger.wallet.daemon.schedulers.observers.{SynchronizationEventReceiver, SynchronizationResult}
import co.ledger.wallet.daemon.services.LogMsgMaker
import co.ledger.wallet.daemon.utils.HexUtils
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.primitives.UnsignedInteger
import com.twitter.inject.Logging
import co.ledger.wallet.daemon.utils.Utils.RichBigInt

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

object Account extends Logging {

  implicit class RichCoreAccount(val a: core.Account) extends AnyVal {
    def erc20Balance(contract: String): Either[Exception, scala.BigInt] =
      Account.erc20Balance(contract, a)

    def erc20Operations(contract: String)(implicit ec: ExecutionContext): Future[List[core.Operation]] =
      Account.erc20Operations(contract, a)

    def erc20Operations(implicit ec: ExecutionContext): Future[List[core.Operation]] =
      Account.erc20Operations(a)

    def erc20Accounts: Either[Exception, List[core.ERC20LikeAccount]] =
      Account.erc20Accounts(a)

    def erc20Account(tokenAddress: String): Either[Exception, core.ERC20LikeAccount] =
      asERC20Account(tokenAddress, a)

    def balance(implicit ec: ExecutionContext): Future[scala.BigInt] =
      Account.balance(a)

    def balances(start: String, end: String, timePeriod: core.TimePeriod)(implicit ec: ExecutionContext): Future[List[scala.BigInt]] =
      Account.balances(start, end, timePeriod, a)

    def firstOperation(implicit ec: ExecutionContext): Future[Option[core.Operation]] =
      Account.firstOperation(a)

    def operationCounts(implicit ec: ExecutionContext): Future[Map[core.OperationType, Int]] =
      Account.operationCounts(a)

    def accountView(walletName: String, cv: CurrencyView)(implicit ec: ExecutionContext): Future[AccountView] =
      Account.accountView(walletName, cv, a)

    def broadcastBTCTransaction(rawTx: Array[Byte], signatures: Seq[BTCSigPub], currentHeight: Long, c: core.Currency): Future[String] =
      Account.broadcastBTCTransaction(rawTx, signatures, currentHeight, a, c)

    def broadcastETHTransaction(rawTx: Array[Byte], signatures: ETHSignature, c: core.Currency)(implicit ec: ExecutionContext): Future[String] =
      Account.broadcastETHTransaction(rawTx, signatures, a, c)

    def createTransaction(transactionInfo: TransactionInfo, c: core.Currency)(implicit ec: ExecutionContext): Future[TransactionView] =
      Account.createTransaction(transactionInfo, a, c)

    def operation(uid: String, fullOp: Int)(implicit ec: ExecutionContext): Future[Option[core.Operation]] =
      Account.operation(uid, fullOp, a)

    def operations(offset: Long, batch: Int, fullOp: Int)(implicit ec: ExecutionContext): Future[Seq[core.Operation]] =
      Account.operations(offset, batch, fullOp, a.queryOperations())

    def operationsCounts(start: Date, end: Date, timePeriod: core.TimePeriod)(implicit ec: ExecutionContext): Future[List[Map[core.OperationType, Int]]] =
      Account.operationsCounts(start, end, timePeriod, a)

    def freshAddresses(implicit ec: ExecutionContext): Future[Seq[core.Address]] =
      Account.freshAddresses(a)

    def sync(poolName: String, walletName: String)(implicit ec: ExecutionContext): Future[SynchronizationResult] =
      Account.sync(poolName, walletName, a)

    def startRealTimeObserver(): Unit =
      Account.startRealTimeObserver(a)

    def stopRealTimeObserver(): Unit =
      Account.stopRealTimeObserver(a)
  }

  def balance(a: core.Account)(implicit ex: ExecutionContext): Future[scala.BigInt] = a.getBalance().map { b =>
    debug(s"Account ${a.getIndex}, balance: ${b}")
    b.toBigInt.asScala
  }

  private def asETHAccount(a: core.Account): Either[Exception, EthereumLikeAccount] = {
    a.getWalletType match {
      case WalletType.ETHEREUM => Right(a.asEthereumLikeAccount())
      case _ => Left(new UnsupportedOperationException("current account is not an ETH account"))
    }
  }

  private def asERC20Account(contract: String, a: core.Account): Either[Exception, ERC20LikeAccount] = {
    asETHAccount(a).flatMap(_.getERC20Accounts.asScala.find(_.getToken.getContractAddress == contract) match {
      case Some(t) => Right(t)
      case None => Left(ERC20NotFoundException(contract))
    })
  }

  def erc20Accounts(a: core.Account): Either[Exception, List[core.ERC20LikeAccount]] =
    asETHAccount(a).map(_.getERC20Accounts.asScala.toList)

  def erc20Balance(contract: String, a: core.Account): Either[Exception, scala.BigInt] =
    asERC20Account(contract, a).map(_.getBalance.asScala)

  def erc20Operations(contract: String, a: core.Account)(implicit ec: ExecutionContext): Future[List[core.Operation]] =
    asERC20Account(contract, a).liftTo[Future].flatMap(erc20Operations)

  private def erc20Operations(a: core.ERC20LikeAccount)(implicit ec: ExecutionContext):
  Future[List[core.Operation]] =
      a.queryOperations().complete().execute().map(_.asScala.toList)

  def erc20Operations(a: core.Account)(implicit ec: ExecutionContext): Future[List[core.Operation]] =
    asETHAccount(a).liftTo[Future].flatMap(_.getERC20Accounts.asScala.toList.traverse(erc20Operations).map(_.flatten))

  def operationCounts(a: core.Account)(implicit ex: ExecutionContext): Future[Map[core.OperationType, Int]] =
    a.queryOperations().addOrder(OperationOrderKey.DATE, true).partial().execute().map { os =>
      os.asScala.groupBy(o => o.getOperationType).map { case (optType, opts) => (optType, opts.size) }
    }

  def accountView(walletName: String, cv: CurrencyView, a: core.Account)(implicit ex: ExecutionContext): Future[AccountView] =
    for {
      b <- balance(a)
      opsCount <- operationCounts(a)
    } yield AccountView(walletName, a.getIndex, b, opsCount, a.getRestoreKey, cv)

  def broadcastBTCTransaction(rawTx: Array[Byte], signatures: Seq[BTCSigPub], currentHeight: Long, a: core.Account, c: core.Currency): Future[String] = {
    c.parseUnsignedBTCTransaction(rawTx, currentHeight) match {
      case Right(tx) =>
        if (tx.getInputs.size != signatures.size) Future.failed(SignatureSizeUnmatchException(tx.getInputs.size(), signatures.size))
        else {
          tx.getInputs.asScala.zipWithIndex.foreach { case (input, index) =>
            input.pushToScriptSig(c.concatSig(signatures(index)._1)) // signature
            input.pushToScriptSig(signatures(index)._2) // pubkey
          }
          debug(s"transaction after sign '${HexUtils.valueOf(tx.serialize())}'")
          a.asBitcoinLikeAccount().broadcastTransaction(tx)
        }
      case Left(m) => Future.failed(new UnsupportedOperationException(s"Account type not supported, can't broadcast BTC transaction: $m"))
    }
  }

  def broadcastETHTransaction(rawTx: Array[Byte], signature: ETHSignature, a: core.Account, c: core.Currency)(implicit ec: ExecutionContext): Future[String] = {
    c.parseUnsignedETHTransaction(rawTx) match {
      case Right(tx) =>
        // calculate the v from chain id
        val v: Long = c.getEthereumLikeNetworkParameters.getChainID.toLong * 2 + 35
        tx.setDERSignature(signature)
        tx.setVSignature(HexUtils.valueOf(v.toHexString))
        a.asEthereumLikeAccount().broadcastTransaction(tx).recoverWith {
          case _ =>
            tx.setVSignature(HexUtils.valueOf((v + 1).toHexString))
            a.asEthereumLikeAccount().broadcastTransaction(tx)
        }
      case Left(m) => Future.failed(new UnsupportedOperationException(s"Account type not supported, can't broadcast ETH transaction: $m"))
    }
  }

  def createTransaction(transactionInfo: TransactionInfo, a: core.Account, c: core.Currency)(implicit ec: ExecutionContext): Future[TransactionView] = {
    (transactionInfo, c.getWalletType) match {
      case (ti: BTCTransactionInfo, WalletType.BITCOIN) =>
        for {
          feesPerByte <- ti.feeAmount match {
            case Some(amount) => Future.successful(c.convertAmount(amount))
            case None => ClientFactory.apiClient.getFees(c.getName).map(f => c.convertAmount(f.getAmount(ti.feeMethod.get)))
          }
          tx <- a.asBitcoinLikeAccount().buildTransaction(false)
            .sendToAddress(c.convertAmount(ti.amount), ti.recipient)
            .pickInputs(BitcoinLikePickingStrategy.OPTIMIZE_SIZE, UnsignedInteger.MAX_VALUE.intValue())
            .setFeesPerByte(feesPerByte)
            .build()
          v <- Bitcoin.newUnsignedTransactionView(tx, feesPerByte.toBigInt.asScala)
        } yield v
      case (ti: ETHTransactionInfo, WalletType.ETHEREUM) =>
        for {
          gasPrice <- ti.gasPrice match {
            case Some(amount) => Future.successful(amount)
            case None => ClientFactory.apiClient.getGasPrice(c.getName)
          }
          gasLimit <- ti.gasLimit match {
            case Some(amount) => Future.successful(amount)
            case None => ClientFactory.apiClient.getGasLimit(c.getName, ti.contract.getOrElse(ti.recipient))
          }
          v <- ti.contract match {
            case Some(contract) =>
              a.asEthereumLikeAccount().getERC20Accounts.asScala.find(_.getToken.getContractAddress == contract) match {
                case Some(erc20Account) =>
                  val balance: scala.BigInt = erc20Account.getBalance.asScala
                  if (balance >= ti.amount) {
                    val inputData = erc20Account.getTransferToAddressData(BigInt.fromIntegerString(ti.amount.toString(10), 10), ti.recipient)
                    val v = a.asEthereumLikeAccount().buildTransaction()
                      .sendToAddress(c.convertAmount(0), contract)
                      .setGasLimit(c.convertAmount(gasLimit))
                      .setGasPrice(c.convertAmount(gasPrice))
                      .setInputData(inputData)
                      .build()
                      .map(UnsignedEthereumTransactionView(_))
                    v
                  } else {
                    Future.failed(ERC20BalanceNotEnough(erc20Account.getToken.getContractAddress, balance, ti.amount))
                  }
                case None => Future.failed(ERC20BalanceNotEnough(contract, 0, ti.amount))
              }
            case None =>
              a.asEthereumLikeAccount().buildTransaction()
                .sendToAddress(c.convertAmount(ti.amount), ti.recipient)
                .setGasLimit(c.convertAmount(gasLimit))
                .setGasPrice(c.convertAmount(gasPrice))
                .build()
                .map(UnsignedEthereumTransactionView(_))
          }
        } yield v
      case _ => Future.failed(new UnsupportedOperationException("Account type not supported, can't create transaction"))
    }
  }

  def operation(uid: String, fullOp: Int, a: core.Account)(implicit ec: ExecutionContext): Future[Option[core.Operation]] = {
    val q = a.queryOperations()
    q.filter().opAnd(core.QueryFilter.operationUidEq(uid))
    (if (fullOp > 0) q.complete().execute()
    else q.partial().execute()).map { ops =>
      debug(s"${ops.size()} returned with uid $uid")
      ops.asScala.headOption
    }
  }

  def firstOperation(a: core.Account)(implicit ec: ExecutionContext): Future[Option[core.Operation]] = {
    a.queryOperations().addOrder(OperationOrderKey.DATE, false).limit(1).partial().execute()
      .map { ops => ops.asScala.toList.headOption }
  }

  def operations(offset: Long, batch: Int, fullOp: Int, query: OperationQuery)(implicit ec: ExecutionContext): Future[Seq[core.Operation]] = {
    (if (fullOp > 0) {
      query.addOrder(OperationOrderKey.DATE, true).offset(offset).limit(batch).complete().execute()
    } else {
      query.addOrder(OperationOrderKey.DATE, true).offset(offset).limit(batch).partial().execute()
    }).map { operations => operations.asScala.toList }
  }

  def balances(start: String, end: String, timePeriod: core.TimePeriod, a: core.Account)(implicit ec: ExecutionContext): Future[List[scala.BigInt]] = {
    a.getBalanceHistory(start, end, timePeriod).map { balances =>
      balances.asScala.toList.map { ba => ba.toBigInt.asScala }
    }
  }

  // TODO: refactor this part once lib-core provides the feature
  def operationsCounts(start: Date, end: Date, timePeriod: core.TimePeriod, a: core.Account)(implicit ec: ExecutionContext): Future[List[Map[core.OperationType, Int]]] = {
    a.queryOperations().addOrder(OperationOrderKey.DATE, true).partial().execute().map { operations =>
      val ops = operations.asScala.toList.filter(op => op.getDate.compareTo(start) >= 0 && op.getDate.compareTo(end) <= 0)
      filter(start, 1, end, standardTimePeriod(timePeriod), ops, Nil)
    }
  }

  @tailrec
  private def filter(start: Date, i: Int, end: Date, timePeriod: Int, operations: List[core.Operation], preResult: List[Map[core.OperationType, Int]]): List[Map[core.OperationType, Int]] = {
    def searchResult(condition: core.Operation => Boolean): Map[core.OperationType, Int] =
      operations.filter(condition).groupBy(op => op.getOperationType).map { case (optType, opts) => (optType, opts.size) }

    val (begin, next) = {
      val calendar = Calendar.getInstance()
      calendar.setTime(start)
      calendar.add(timePeriod, i - 1)
      val begin = calendar.getTime
      calendar.add(timePeriod, 1)
      (begin, calendar.getTime)
    }
    if (end.after(next)) {
      val result = searchResult(op => op.getDate.compareTo(begin) >= 0 && op.getDate.compareTo(next) < 0)
      filter(start, i + 1, end, timePeriod, operations, preResult ::: List(result))
    } else {
      val result = searchResult(op => op.getDate.compareTo(begin) >= 0 && op.getDate.compareTo(end) <= 0)
      preResult ::: List(result)
    }
  }

  private def standardTimePeriod(timePeriod: core.TimePeriod): Int = timePeriod match {
    case core.TimePeriod.DAY => Calendar.DATE
    case core.TimePeriod.MONTH => Calendar.MONTH
    case core.TimePeriod.WEEK => Calendar.WEEK_OF_MONTH
  }

  def freshAddresses(a: core.Account)(implicit ec: ExecutionContext): Future[Seq[core.Address]] = {
    a.getFreshPublicAddresses().map(_.asScala.toList)
  }

  def sync(poolName: String, walletName: String, a: core.Account)(implicit ec: ExecutionContext): Future[SynchronizationResult] = {
    val promise: Promise[SynchronizationResult] = Promise[SynchronizationResult]()
    val receiver: core.EventReceiver = new SynchronizationEventReceiver(a.getIndex, walletName, poolName, promise)
    a.synchronize().subscribe(LedgerCoreExecutionContext(ec), receiver)
    debug(s"Synchronize $a")
    val f = promise.future
    f onComplete (_ => a.getEventBus.unsubscribe(receiver))
    f
  }

  def startRealTimeObserver(a: core.Account): Unit = {
    if (DaemonConfiguration.realTimeObserverOn && !a.isObservingBlockchain) a.startBlockchainObservation()
    debug(LogMsgMaker.newInstance(s"Set real time observer on ${a.isObservingBlockchain}").append("account", a).toString())
  }

  def stopRealTimeObserver(a: core.Account): Unit = {
    debug(LogMsgMaker.newInstance("Stop real time observer").append("account", a).toString())
    if (a.isObservingBlockchain) a.stopBlockchainObservation()
  }


  class Derivation(private val accountCreationInfo: core.AccountCreationInfo) {
    val index: Int = accountCreationInfo.getIndex

    lazy val view: AccountDerivationView = {
      val paths = accountCreationInfo.getDerivations.asScala
      val owners = accountCreationInfo.getOwners.asScala
      val pubKeys = {
        val pks = accountCreationInfo.getPublicKeys
        if (pks.isEmpty) {
          paths.map { _ => "" }
        }
        else {
          pks.asScala.map(HexUtils.valueOf)
        }
      }
      val chainCodes = {
        val ccs = accountCreationInfo.getChainCodes
        if (ccs.isEmpty) {
          paths.map { _ => "" }
        }
        else {
          ccs.asScala.map(HexUtils.valueOf)
        }
      }
      val derivations = paths.indices.map { i =>
        DerivationView(
          paths(i),
          owners(i),
          pubKeys(i) match {
            case "" => None
            case pubKey => Option(pubKey)
          },
          chainCodes(i) match {
            case "" => None
            case chainCode => Option(chainCode)
          })
      }
      AccountDerivationView(index, derivations)
    }
  }

  class ExtendedDerivation(info: core.ExtendedKeyAccountCreationInfo) {
    val index: Int = info.getIndex
    lazy val view: AccountExtendedDerivationView = {
      val extKeys = info.getExtendedKeys.asScala.map(Option.apply).padTo(info.getDerivations.size(), None)
      val derivations = (info.getDerivations.asScala, info.getOwners.asScala, extKeys).zipped map {
        case (path, owner, key) =>
          ExtendedDerivationView(path, owner, key)
      }
      AccountExtendedDerivationView(index, derivations.toList)
    }
  }

  def newDerivation(coreD: core.AccountCreationInfo): Derivation = {
    new Derivation(coreD)
  }
}

case class AccountView(
                        @JsonProperty("wallet_name") walletName: String,
                        @JsonProperty("index") index: Int,
                        @JsonProperty("balance") balance: scala.BigInt,
                        @JsonProperty("operation_count") operationCounts: Map[core.OperationType, Int],
                        @JsonProperty("keychain") keychain: String,
                        @JsonProperty("currency") currency: CurrencyView
                      )

case class ERC20AccountView(
                             @JsonProperty("contract_address") contractAddress: String,
                             @JsonProperty("name") name: String,
                             @JsonProperty("number_of_decimal") numberOrDecimal: Int,
                             @JsonProperty("symbol") symbol: String,
                             @JsonProperty("balance") balance: scala.BigInt
                           )

object ERC20AccountView {
  def apply(erc20Account: ERC20LikeAccount): ERC20AccountView = {
    ERC20AccountView(
      erc20Account.getToken.getContractAddress,
      erc20Account.getToken.getName,
      erc20Account.getToken.getNumberOfDecimal,
      erc20Account.getToken.getSymbol,
      erc20Account.getBalance.asScala
    )
  }
}

case class DerivationView(
                           @JsonProperty("path") path: String,
                           @JsonProperty("owner") owner: String,
                           @JsonProperty("pub_key") pubKey: Option[String],
                           @JsonProperty("chain_code") chainCode: Option[String]
                         )

case class AccountDerivationView(
                                  @JsonProperty("account_index") accountIndex: Int,
                                  @JsonProperty("derivations") derivations: Seq[DerivationView]
                                )

case class ExtendedDerivationView(
                                   @JsonProperty("path") path: String,
                                   @JsonProperty("owner") owner: String,
                                   @JsonProperty("extended_key") extKey: Option[String],
                                 )

case class AccountExtendedDerivationView(
                                          @JsonProperty("account_index") accountIndex: Int,
                                          @JsonProperty("derivations") derivations: Seq[ExtendedDerivationView]
                                        )

case class FreshAddressView(
                             @JsonProperty("address") address: String,
                             @JsonProperty("derivation_path") derivation: String
                           )