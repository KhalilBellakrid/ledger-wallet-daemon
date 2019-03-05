package co.ledger.wallet.daemon.models

import co.ledger.core
import co.ledger.wallet.daemon.models.coins.Coin.NetworkParamsView
import co.ledger.wallet.daemon.models.coins.{Bitcoin, EthereumNetworkParamView}
import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.JavaConverters._

object Currency {

  implicit class RichCoreCurrency(val c: core.Currency) extends AnyVal {
    def concatSig(sig: Array[Byte]): Array[Byte] = Currency.concatSig(c)(sig)

    def parseUnsignedBTCTransaction(rawTx: Array[Byte], currentHeight: Long): Either[String, core.BitcoinLikeTransaction] = Currency.parseUnsignedBTCTransaction(c)(rawTx, currentHeight)

    def parseUnsignedETHTransaction(rawTx: Array[Byte]): Either[String, core.EthereumLikeTransaction] = Currency.parseUnsignedETHTransaction(c)(rawTx)

    def validateAddress(address: String): Boolean = Currency.validateAddress(c)(address)

    def convertAmount(amount: BigInt): core.Amount = Currency.convertAmount(c)(amount)

    def currencyView: CurrencyView = Currency.currencyView(c)
  }

  def concatSig(currency: core.Currency)(sig: Array[Byte]): Array[Byte] = currency.getWalletType match {
    case core.WalletType.BITCOIN => sig ++ currency.getBitcoinLikeNetworkParameters.getSigHash
    case _ => sig
  }

  def parseUnsignedBTCTransaction(currency: core.Currency)(rawTx: Array[Byte], currentHeight: Long): Either[String, core.BitcoinLikeTransaction] =
    currency.getWalletType match {
      case core.WalletType.BITCOIN => Right(core.BitcoinLikeTransactionBuilder.parseRawUnsignedTransaction(currency, rawTx, currentHeight.toInt))
      case w => Left(s"$w is not Bitcoin")
    }

  def parseUnsignedETHTransaction(currency: core.Currency)(rawTx: Array[Byte]): Either[String, core.EthereumLikeTransaction] =
    currency.getWalletType match {
      case core.WalletType.ETHEREUM => Right(core.EthereumLikeTransactionBuilder.parseRawUnsignedTransaction(currency, rawTx))
      case w => Left(s"$w is not Ethereum")
    }

  def validateAddress(c: core.Currency)(address: String): Boolean = core.Address.isValid(address, c)

  def convertAmount(c: core.Currency)(amount: BigInt): core.Amount = core.Amount.fromHex(c, amount.toString(16))

  def currencyView(c: core.Currency): CurrencyView = CurrencyView(
    c.getName,
    c.getWalletType,
    c.getBip44CoinType,
    c.getPaymentUriScheme,
    c.getUnits.asScala.map(newUnitView),
    newNetworkParamsView(c)
  )

  private def newUnitView(coreUnit: core.CurrencyUnit): UnitView =
    UnitView(coreUnit.getName, coreUnit.getSymbol, coreUnit.getCode, coreUnit.getNumberOfDecimal)

  private def newNetworkParamsView(coreCurrency: core.Currency): NetworkParamsView = coreCurrency.getWalletType match {
    case core.WalletType.BITCOIN => Bitcoin.newNetworkParamsView(coreCurrency.getBitcoinLikeNetworkParameters)
    case core.WalletType.ETHEREUM => EthereumNetworkParamView(coreCurrency.getEthereumLikeNetworkParameters)
    case core.WalletType.RIPPLE =>
      UnsupportedNetworkParamsView("RIPPLE", "wallet type RIPPLE is not supported by wallet daemon")
    case core.WalletType.MONERO =>
      UnsupportedNetworkParamsView("MONERO", "wallet type MONERO is not supported by wallet daemon")
  }
}

case class UnsupportedNetworkParamsView(walletType: String, error: String) extends NetworkParamsView

case class CurrencyView(
                         @JsonProperty("name") name: String,
                         @JsonProperty("family") family: core.WalletType,
                         @JsonProperty("bip_44_coin_type") bip44CoinType: Int,
                         @JsonProperty("payment_uri_scheme") paymentUriScheme: String,
                         @JsonProperty("units") units: Seq[UnitView],
                         @JsonProperty("network_params") networkParams: NetworkParamsView
                       )

case class UnitView(
                     @JsonProperty("name") name: String,
                     @JsonProperty("symbol") symbol: String,
                     @JsonProperty("code") code: String,
                     @JsonProperty("magnitude") magnitude: Int
                   )
