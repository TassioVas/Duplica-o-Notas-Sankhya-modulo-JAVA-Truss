package br.com.doalti.sankhya.truss;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.util.FinderWrapper;
import br.com.sankhya.jape.util.JapeSessionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.PrePersistEntityState;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.centrais.CACHelper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.ws.ServiceContext;

public class gerarLote implements AcaoRotinaJava {

	AuthenticationInfo authInfo;
	ServiceContext sctx;
	BigDecimal nuNotaOrig;
	BigDecimal nuNota = BigDecimal.ZERO;
	BigDecimal codProdIte;
	BigDecimal codLocalOrigIte;
	BigDecimal codEmpIte;
	BigDecimal topDestino;
	BigDecimal codEmp;
	BigDecimal nuTab;
	BigDecimal codCencus;
	BigDecimal top;
	BigDecimal parceiroNo;
	BigDecimal emp;
	BigDecimal parc;
	String statusNota;
	BigDecimal count;
	BigDecimal codProdOrig;
	BigDecimal codProd;
	BigDecimal sequencia;
	BigDecimal qtdVol;

	String volume = "";
	FinderWrapper finde;
	String msg = "";
	String controleIte = "";
	String adEspelho = "";
	BigDecimal qtdNeg;

	@Override
	public void doAction(ContextoAcao ctx) throws Exception {
		System.out.println("inciou o codigo");

		IncluirGerarLote(ctx);

	}

	@SuppressWarnings("unlikely-arg-type")
	private void IncluirGerarLote(ContextoAcao ctx) throws Exception {
		System.out.println("Entrou no metodo");
		Timestamp dataHoraAtual = new Timestamp(System.currentTimeMillis());
		JdbcWrapper jdbc = JapeFactory.getEntityFacade().getJdbcWrapper();
		NativeSql nativeSql = new NativeSql(jdbc);
		SessionHandle hnd = JapeSession.open();
		EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
		CACHelper cacHelper = new CACHelper();

		for (int i = 0; i < (ctx.getLinhas()).length; i++) {
			Registro linha = ctx.getLinhas()[i];
			BigDecimal nuNota = (BigDecimal) linha.getCampo("NUNOTA");

			ResultSet rsControle = nativeSql.executeQuery("SELECT CONTROLE FROM TGFITE WHERE NUNOTA = " + nuNota);

			while (rsControle.next()) {
				controleIte = rsControle.getString("CONTROLE");

				ResultSet result = nativeSql.executeQuery(
						" SELECT AD_ESPELHO, AD_NUNOTAESPORIG, STATUSNOTA  FROM TGFCAB WHERE NUNOTA = " + nuNota);

				System.out.println("depois do result set : " + nuNota);

				while (result.next()) {
					adEspelho = result.getString("AD_ESPELHO");
					System.out.println("Ad espelho " + adEspelho);
					statusNota = result.getString("STATUSNOTA");

					if (adEspelho.equals("S") && statusNota.equals("A") || statusNota.equals("L")) {

						ResultSet rs = nativeSql
								.executeQuery(" SELECT AD_ESPELHO, AD_NUNOTAESPORIG, CODEMP, QTDVOL, VOLUME "
										+ " FROM TGFCAB " + "	WHERE NUNOTA = " + nuNota);

						while (rs.next()) {

							nuNotaOrig = rs.getBigDecimal("AD_NUNOTAESPORIG");
							System.out.println("" + nuNotaOrig);
							codEmp = rs.getBigDecimal("CODEMP");
							System.out.println(" codEmp empresa da tela " + codEmp);
							qtdVol = rs.getBigDecimal("QTDVOL");
							volume = rs.getString("VOLUME");

							JapeWrapper cabDAO = JapeFactory.dao(DynamicEntityNames.CABECALHO_NOTA);
							cabDAO.prepareToUpdateByPK(nuNotaOrig).set("QTDVOL", qtdVol).set("VOLUME", volume).update();

							System.out.println("depois do result set cpfcnpj : " + nuNotaOrig);

							String queryIte = ("SELECT " + " QTDNEG, SEQUENCIA , " + " CODPROD,  " + " CODLOCALORIG,"
									+ " CODEMP as CODEMPORIG FROM TGFITE WHERE NUNOTA = " + nuNota);
							// select para pegar os campos na ite para validar o estoque

							System.out.println("sysout String Rs : " + queryIte);
							ResultSet rsITE = nativeSql.executeQuery(queryIte);

							while (rsITE.next()) { // executando e capurando os campos
								qtdNeg = rsITE.getBigDecimal("QTDNEG");
								System.out.println("ite QTDNEG ITE: " + qtdNeg);
								// controleIte = rsITE.getString("CONTROLE");
								System.out.println("ite controle : " + controleIte);
								codLocalOrigIte = rsITE.getBigDecimal("CODLOCALORIG");
								System.out.println("ite local origem : " + codLocalOrigIte);
								codEmpIte = rsITE.getBigDecimal("CODEMPORIG");
								System.out.println("ite EMP : " + codEmpIte);
								codProd = rsITE.getBigDecimal("CODPROD");
								sequencia = rsITE.getBigDecimal("SEQUENCIA");
								System.out.println("ite codprod filha : " + codProd);

								JapeWrapper iteDAO = JapeFactory.dao(DynamicEntityNames.ITEM_NOTA);
								iteDAO.prepareToUpdateByPK(nuNotaOrig, sequencia).set("CODPROD", codProd)
										.set("QTDNEG", qtdNeg).set("CODLOCALORIG", codLocalOrigIte)
										.set("CONTROLE", controleIte).update();

							
								ctx.setMensagemRetorno("Lote gerado com sucesso nota Pai :" + nuNotaOrig);
							
							}
						}

					} else {
						ctx.setMensagemRetorno("pedido ainda nao aprovado, ou incluido o lote.");
					}
				}
			}
		}
	}
}
