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
					//
					if (adEspelho.equals("S") && statusNota.equals("A") || statusNota.equals("L")) {

						ResultSet rs = nativeSql.executeQuery(" SELECT AD_ESPELHO, AD_NUNOTAESPORIG, CODEMP "
								+ " FROM TGFCAB " + "	WHERE NUNOTA = " + nuNota);

						while (rs.next()) {

							nuNotaOrig = rs.getBigDecimal("AD_NUNOTAESPORIG");
							System.out.println("" + nuNotaOrig);
							codEmp = rs.getBigDecimal("CODEMP");
							System.out.println(" codEmp empresa da tela " + codEmp);

							System.out.println("depois do result set cpfcnpj : " + nuNotaOrig);
							
							String queryEst = ("SELECT COUNT(*) AS CONTADOR FROM TGFEST " + " WHERE "
									+ " CODPROD = " + codProd + " AND CONTROLE = '" + controleIte + "'"
									+ " AND CODLOCAL = " + codLocalOrigIte + " AND CODEMP = " + codEmp);
							System.out.println("sysout String Rs : " + queryEst);
							ResultSet rsEst = nativeSql.executeQuery(queryEst);
							System.out.println("antes do if a top destino : " + topDestino);

							
								while (rsEst.next() && rsEst.getInt("CONTADOR") == 0) {
									ctx.setMensagemRetorno(" Estque insuficiente! ");
									System.out.println("passou aqui no if do estoque");
									return;
							     }

							String queryIte = ("SELECT " + " QTDNEG, " + " CODPROD, " + " CODLOCALORIG, "
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
								System.out.println("ite codprod filha : " + codProd);

								System.out.println("Entrou no else");
								String queryCont = ("SELECT count(CONTROLE) AS CONTADOR FROM TGFITE WHERE NUNOTA = "
										+ this.nuNotaOrig);

								ResultSet rsCont = nativeSql.executeQuery(queryCont);

								while (rsCont.next()) {
									// count = rsCont.getBigDecimal("CONTADOR");
									if (this.controleIte != null || rsCont.getInt("CONTADOR") > 0) {
										System.out.println("Sysout entrou no contador");

										
											String queryItem = ("SELECT CODPROD, SEQUENCIA FROM TGFITE WHERE NUNOTA = "
													+ this.nuNotaOrig);

											ResultSet rsItem = nativeSql.executeQuery(queryItem);

											while (rsItem.next()) {
												codProdOrig = rsItem.getBigDecimal("CODPROD");
												sequencia = rsItem.getBigDecimal("SEQUENCIA");
												// while (codProdOrig == codProd) {
												System.out.println("Sysout entrou no while");

												JapeWrapper dlt = JapeFactory.dao(DynamicEntityNames.ITEM_NOTA);
												dlt.delete(nuNotaOrig, sequencia);

												this.sctx = new ServiceContext(null);
												authInfo = new AuthenticationInfo("SUP", BigDecimal.ZERO,
														BigDecimal.ZERO, new Integer(2147483647));
												this.sctx.setAutentication(authInfo);
												this.sctx.putHttpSessionAttribute("usuario_logado",
														authInfo.getUserID());
												JapeSessionContext.putProperty("usuario_logado", authInfo.getUserID());
												this.sctx.setAttribute("usuario_logado", authInfo.getUserID());
												this.sctx.makeCurrent();

												System.out.println("auth : " + authInfo);

												try {
													dwfFacade = EntityFacadeFactory.getDWFFacade();
													finde = new FinderWrapper(" ItemNota", " NUNOTA = " + nuNota);
													System.out.println("SYSOUT nunota origem incluir item  " + nuNota);
													Collection<DynamicVO> itemAtualITE = dwfFacade
															.findByDynamicFinderAsVO(finde);
													BigDecimal codLocal = BigDecimal.ZERO;
													for (DynamicVO item1 : itemAtualITE) {
														try {
															Collection<PrePersistEntityState> itensNota = new ArrayList<>();
															DynamicVO itemVO = (DynamicVO) dwfFacade
																	.getDefaultValueObjectInstance("ItemNota");
															itemVO.setProperty("SEQUENCIA",
																	item1.asBigDecimal("SEQUENCIA"));
															System.out.println("Depois do sequenc");
															itemVO.setProperty("CODPROD",
																	item1.asBigDecimal("CODPROD"));
															System.out.println("depois do codprod");
															itemVO.setProperty("CODVOL", item1.asString("CODVOL"));
															System.out.println("depois do codvol");
															itemVO.setProperty("CODLOCALORIG",
																	item1.asBigDecimal("CODLOCALORIG"));
															System.out.println("depois do qtdneg");
															itemVO.setProperty("QTDNEG", item1.asBigDecimal("QTDNEG"));
															System.out.println("Antes do vlrunit");
															itemVO.setProperty("VLRUNIT",
																	item1.asBigDecimal("VLRUNIT"));
															System.out.println("Antes do c");
															// itemVO.setProperty("CONTROLE",
															// item1.asString("CONTROLE"));
															itemVO.setProperty("CONTROLE", item1.asString("CONTROLE"));

															System.out.println("Antes do Persist");
															PrePersistEntityState itePreState = PrePersistEntityState
																	.build(dwfFacade, "ItemNota", itemVO);
															System.out.println("Antes do Collection ITE");
															itensNota.add(itePreState);
															System.out.println("nunota inclusao " + nuNotaOrig);
															cacHelper.incluirAlterarItem(nuNotaOrig, authInfo,
																	itensNota, true);

														} catch (Exception e) {
															e.printStackTrace();
															msg = "Erro na inclusao do item " + e.getMessage();
															System.out.println(msg);
															ctx.setMensagemRetorno(e.getMessage());
														}

														/*
														 * JapeWrapper iteDAO =
														 * JapeFactory.dao(DynamicEntityNames.ITEM_NOTA);
														 * iteDAO.prepareToUpdateByPK(nuNotaOrig,
														 * sequencia).set("CONTROLE", this.controleIte) .set("QTDNEG",
														 * this.qtdNeg).update();
														 */
														ctx.setMensagemRetorno(
																"Lote gerado com sucesso nota Pai :" + nuNotaOrig);
													}
												} catch (Exception e) {
													e.printStackTrace();
													msg = "Erro na inclusao do item " + e.getMessage();
													System.out.println(msg);
													ctx.setMensagemRetorno(e.getMessage());
												}
											}
									} else {
										ctx.setMensagemRetorno("pedido ainda nao aprovado, ou incluido o lote.");
									}
								}
							}
						}
					} else {
						ctx.setMensagemRetorno(
								"Não existe pedido espelho " + "para o documento selecionado!\n Status nao aprovado!");
					}
				}
			}
		}
}}
