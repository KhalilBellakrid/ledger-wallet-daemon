package co.ledger.wallet.daemon.services

import java.util.{Date, UUID}

import cats.data.OptionT
import cats.instances.future._
import cats.instances.list._
import cats.instances.option._
import cats.syntax.either._
import cats.syntax.traverse._
import co.ledger.core
import co.ledger.wallet.daemon.async.MDCPropagatingExecutionContext.Implicits.global
import co.ledger.wallet.daemon.clients.ApiClient.FallbackParams
import co.ledger.wallet.daemon.clients.ClientFactory
import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import co.ledger.wallet.daemon.database.DaemonCache
import co.ledger.wallet.daemon.models.Account._
import co.ledger.wallet.daemon.models.Currency._
import co.ledger.wallet.daemon.models.Operations.{OperationView, PackedOperationsView}
import co.ledger.wallet.daemon.models.Wallet._
import co.ledger.wallet.daemon.models._
import co.ledger.wallet.daemon.schedulers.observers.SynchronizationResult
import co.ledger.wallet.daemon.utils.Utils.{RichBigInt, _}
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response}
import javax.inject.{Inject, Singleton}
import org.web3j.abi.datatypes.{Address, Function, Type, Uint}
import org.web3j.abi.{FunctionEncoder, TypeReference}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.parsing.json.JSON
import scala.util.{Success, Try}

@Singleton
class AccountsService @Inject()(daemonCache: DaemonCache) extends DaemonService {

  def accounts(walletInfo: WalletInfo): Future[Seq[AccountView]] = {
    daemonCache.withWallet(walletInfo) { wallet =>
      wallet.accounts.flatMap { as =>
        as.toList.map(a => a.accountView(walletInfo.walletName, wallet.getCurrency.currencyView)).sequence[Future, AccountView]
      }
    }
  }

  def account(accountInfo: AccountInfo): Future[Option[AccountView]] = {
    daemonCache.withWallet(accountInfo.walletInfo) { wallet =>
      wallet.account(accountInfo.accountIndex).flatMap(ao =>
        ao.map(_.accountView(accountInfo.walletName, wallet.getCurrency.currencyView)).sequence)
    }
  }

  /**
    * Method to synchronize account operations from public resources. The method may take a while
    * to finish. This method only synchronize a single account.
    *
    * @return a Future of sequence of result of synchronization.
    */
  def synchronizeAccount(accountInfo: AccountInfo): Future[Seq[SynchronizationResult]] =
    daemonCache.withAccount(accountInfo)(_.sync(accountInfo.poolName, accountInfo.walletName).map(Seq(_)))

  def getAccount(accountInfo: AccountInfo): Future[Option[core.Account]] = {
    daemonCache.getAccount(accountInfo: AccountInfo)
  }

  def getBalance(contract: Option[String], accountInfo: AccountInfo): Future[BigInt] = {

    def encodeBalanceFunction(address: String): Try[String] = {
      info(s"Try to encode balance function with address: $address")
      val function = new Function(
        "balanceOf",
        List[Type[_]](new Address(address.replaceFirst("0x", ""))).asJava,
        List[TypeReference[_ <: Type[_]]](new TypeReference[Uint]() {}).asJava
      )
      Try(FunctionEncoder.encode(function))
    }

    def runWithFallback(cb: Future[BigInt], fallback: FallbackParams, client: Service[Request, Response], timeout: Duration): Future[BigInt] = {
      Future(Await.result(cb, timeout)).recoverWith { case t =>
        warn(s"Failed to get balance from libcore: $t")
        info("Using fallback provider")
        val result = for {
          address <- OptionT(accountFreshAddresses(accountInfo).map(_.headOption.map(_.address)))
          body <- OptionT.liftF(Future.fromTry(contract match {
            case Some(contractAddress) =>
              encodeBalanceFunction(address).map { data =>
                s"""{"jsonrpc":"2.0","method":"eth_call","params":[{"to": "$contractAddress", "data": "$data"}, "latest"],"id":1}"""
              }
            case None =>
              Success(s"""{"jsonrpc":"2.0","method":"eth_getBalance","params":["$address", "latest"],"id":1}""")
          }))
          response <- {
            val request = Request(Method.Post, fallback.query).host(fallback.host)

            request.setContentString(body)
            request.setContentType("application/json")

            OptionT.liftF(client(request).asScala())
          }
          result <- OptionT.liftF(Future.fromTry(Try {
            JSON.parseFull(response.contentString).get.asInstanceOf[Map[String, Any]] match {
              case fields: Map[String, Any] => BigInt(fields("result").asInstanceOf[String].replaceFirst("0x", ""), 16)
              case _ => throw new Exception("Failed to parse fallback provider result")
            }
          }))
        } yield result
        result.getOrElseF(Future.failed(new Exception("Unable to fetch from fallback provider")))
      }
    }

    val balance = daemonCache.withAccount(accountInfo)(a => contract match {
      case Some(c) => a.erc20Balance(c)
      case None => a.balance
    })

    daemonCache.withWallet(accountInfo.walletInfo) { wallet =>
      ClientFactory.apiClient.fallbackClient(wallet.getCurrency.getName) match {
        case Some((fallback, client)) =>
          val timeout = DaemonConfiguration.explorer.api.fallbackTimeout.milliseconds
          runWithFallback(balance, fallback, client, timeout)
        case None =>
          balance
      }
    }
  }

  def getXpub(accountInfo: AccountInfo): Future[String] =
    daemonCache.withAccount(accountInfo) { a => Future.successful(a.getRestoreKey) }

  def getERC20Operations(tokenAccountInfo: TokenAccountInfo): Future[List[OperationView]] =
    daemonCache.withAccountAndWallet(tokenAccountInfo.accountInfo) {
      case (account, wallet) =>
        account.erc20Operations(tokenAccountInfo.tokenAddress).flatMap { operations =>
          operations.traverse { case (coreOp, erc20Op) =>
            Operations.getErc20View(erc20Op, coreOp, wallet, account)
          }
        }
    }

  def getERC20Operations(accountInfo: AccountInfo): Future[List[OperationView]] =
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        account.erc20Operations.flatMap { operations =>
          operations.traverse { case (coreOp, erc20Op) =>
            Operations.getErc20View(erc20Op, coreOp, wallet, account)
          }
        }
    }
  
  def getBatchedERC20Operations(accountInfo: AccountInfo, offset: Long, batch: Int): Future[List[OperationView]] =
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        account.batchedErc20Operations(offset, batch).flatMap { operations =>
          operations.traverse { case (coreOp, erc20Op) =>
            Operations.getErc20View(erc20Op, coreOp, wallet, account)
          }
        }
    }

  def getTokenAccounts(accountInfo: AccountInfo): Future[List[ERC20AccountView]] =
    daemonCache.withAccount(accountInfo) { account =>
      for {
        erc20Accounts <- account.erc20Accounts.liftTo[Future]
        views <- erc20Accounts.map(ERC20AccountView.fromERC20Account).sequence
      } yield views
    }

  def getTokenAccount(tokenAccountInfo: TokenAccountInfo): Future[ERC20AccountView] =
    daemonCache.withAccount(tokenAccountInfo.accountInfo) { account =>
      for {
        erc20Account <- account.erc20Account(tokenAccountInfo.tokenAddress).liftTo[Future]
        view <- ERC20AccountView.fromERC20Account(erc20Account)
      } yield view
    }

  def getTokenCoreAccount(tokenAccountInfo: TokenAccountInfo): Future[core.ERC20LikeAccount] =
    daemonCache.withAccount(tokenAccountInfo.accountInfo)(_.erc20Account(tokenAccountInfo.tokenAddress).liftTo[Future])

  def getTokenCoreAccountBalanceHistory(tokenAccountInfo: TokenAccountInfo, startDate: Date, endDate: Date, period: core.TimePeriod): Future[List[BigInt]] = {
    getTokenCoreAccount(tokenAccountInfo).map(_.getBalanceHistoryFor(startDate, endDate, period).asScala.map(_.asScala).toList)
  }

  def accountFreshAddresses(accountInfo: AccountInfo): Future[Seq[FreshAddressView]] = {
    daemonCache.getFreshAddresses(accountInfo)
  }

  def accountDerivationPath(accountInfo: AccountInfo): Future[String] =
    daemonCache.withWallet(accountInfo.walletInfo)(_.accountDerivationPathInfo(accountInfo.accountIndex))

  def nextAccountCreationInfo(accountIndex: Option[Int], walletInfo: WalletInfo): Future[AccountDerivationView] =
    daemonCache.withWallet(walletInfo)(_.accountCreationInfo(accountIndex)).map(_.view)

  def nextExtendedAccountCreationInfo(accountIndex: Option[Int], walletInfo: WalletInfo): Future[AccountExtendedDerivationView] =
    daemonCache.withWallet(walletInfo)(_.accountExtendedCreation(accountIndex)).map(_.view)

  def accountOperations(queryParams: OperationQueryParams, accountInfo: AccountInfo): Future[PackedOperationsView] = {
    (queryParams.next, queryParams.previous) match {
      case (Some(n), _) =>
        // next has more priority, using database batch instead queryParams.batch
        info(LogMsgMaker.newInstance("Retrieve next batch operation").toString())
        daemonCache.getNextBatchAccountOperations(n, queryParams.fullOp, accountInfo)
      case (_, Some(p)) =>
        info(LogMsgMaker.newInstance("Retrieve previous operations").toString())
        daemonCache.getPreviousBatchAccountOperations(p, queryParams.fullOp, accountInfo)
      case _ =>
        // new request
        info(LogMsgMaker.newInstance("Retrieve latest operations").toString())
        daemonCache.getAccountOperations(queryParams.batch, queryParams.fullOp, accountInfo)
    }
  }

  def firstOperation(accountInfo: AccountInfo): Future[Option[OperationView]] = {
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        account.firstOperation flatMap {
          case None => Future(None)
          case Some(o) => Operations.getView(o, wallet, account).map(Some(_))
        }
    }
  }

  def accountOperation(uid: String, fullOp: Int, accountInfo: AccountInfo): Future[Option[OperationView]] =
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        for {
          operationOpt <- account.operation(uid, fullOp)
          op <- operationOpt match {
            case None => Future.successful(None)
            case Some(op) => Operations.getView(op, wallet, account).map(Some(_))
          }
        } yield op
    }

  def createAccount(accountCreationBody: AccountDerivationView, walletInfo: WalletInfo): Future[AccountView] =
    daemonCache.withWallet(walletInfo) {
      w => w.addAccountIfNotExist(accountCreationBody).flatMap(_.accountView(walletInfo.walletName, w.getCurrency.currencyView))
    }

  def createAccountWithExtendedInfo(derivations: AccountExtendedDerivationView, walletInfo: WalletInfo): Future[AccountView] =
    daemonCache.withWallet(walletInfo) {
      w => w.addAccountIfNotExist(derivations).flatMap(_.accountView(walletInfo.walletName, w.getCurrency.currencyView))
    }

}

case class OperationQueryParams(previous: Option[UUID], next: Option[UUID], batch: Int, fullOp: Int)
