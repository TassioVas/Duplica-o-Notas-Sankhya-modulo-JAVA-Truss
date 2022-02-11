package br.com.doalti.sankhya.truss;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.sankhya.util.XMLUtils;

import br.com.sankhya.commons.xml.Element;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.EntityPrimaryKey;
import br.com.sankhya.jape.dao.EntityPropertyDescriptor;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.util.FinderWrapper;
import br.com.sankhya.jape.util.JapeSessionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.jape.vo.PrePersistEntityState;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.BarramentoRegra;
import br.com.sankhya.modelcore.comercial.CentralItemNota;
import br.com.sankhya.modelcore.comercial.ComercialUtils;
import br.com.sankhya.modelcore.comercial.centrais.CACHelper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.ws.ServiceContext;

public class EspelhoNota implements AcaoRotinaJava {

	public AuthenticationInfo authInfo;
	private ServiceContext sctx;
	private static final SimpleDateFormat ddMMyyy = new SimpleDateFormat("dd/MM/yyyy");
	private Collection<BigDecimal> notasSelecao;
	private String sqlVerificaBonificacao;
	private String sqlBrindeCliPremi;
	private static AuthenticationInfo auth;
	private BigDecimal nuNotaPub;
	private BigDecimal numNota;
	private CentralItemNota centralItemNota;
	private String sqlFaturaNota;
	private String erro = "0";
	private CACHelper cacHelper = null;
	private BigDecimal nuNota = BigDecimal.ZERO;
	private BigDecimal nuNotaOrig = BigDecimal.ZERO;
	private BigDecimal nuNotaOld = BigDecimal.ZERO;
	private BigDecimal codLocal = BigDecimal.ZERO;
	private BigDecimal codEmp;
	private BigDecimal codEmpNegoc;
	private BigDecimal codParc;
	private BigDecimal id;
	private BigDecimal codProd;
	private BigDecimal qtdNeg;
	private BigDecimal vlrUnit;
	private BigDecimal vlrDesdob;
	private BigDecimal codTipOper;
	private BigDecimal codemp;
	private BigDecimal parcOrig;
	private BigDecimal parcOrigEsp;
	private BigDecimal topOrig;
	private BigDecimal topOrigEsp;
	private BigDecimal topDestino;
	private BigDecimal parcDestino;
	private BigDecimal quantidade;
	private BigDecimal vlr;
	private BigDecimal codCencus;
	private BigDecimal codCencusOrig;
	private BigDecimal codNat;
	private BigDecimal codtioper;
	private BigDecimal codVend;
	private BigDecimal count;
	private BigDecimal codProdIte;
	private BigDecimal codLocalOrigIte;
	private BigDecimal codEmpIte;
	private BigDecimal qtdVol;
	private BigDecimal codParcTransp;
	private BigDecimal pacSedex;
	
	private String tipFrete = "";
	private String observacao = "";
	private String obsInt = "";
	private String volume = "";
	private String controleIte = "";
	private String tipmov = "";
	private String CODBAI;
	private String CODCID;
	private String msg = " ";
	private String nome_razao_social;
	private String cnpj_cpf;
	private String obs = "";
	private String codEnd;
	private String obsInterno = "";
	private String produto = "";
	private BigDecimal codTipVenda;
	private String adEsp = "";
	private String ativo = "";

	JdbcWrapper jdbc = null;
	JapeSession.SessionHandle hnd = null;
	private String erroItem = "";

	Collection dynamicVOs;
	EntityFacade dwfFacade;
	FinderWrapper finde;
	private String cep;
	private String resposta;

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		System.out.println("começou a rodar o codigo");

		this.cacHelper = new CACHelper();// ??
		this.sctx = new ServiceContext(null);// ??

		// this.sctx.setAutentication(authInfo);// ??
		// this.sctx.putHttpSessionAttribute("usuario_logado", authInfo.getUserID());
		// JapeSessionContext.putProperty("usuario_logado", authInfo.getUserID());
		// this.sctx.setAttribute("usuario_logado", authInfo.getUserID());
		this.sctx.makeCurrent();
		dwfFacade = EntityFacadeFactory.getDWFFacade();
		System.out.println("antes de rodar o metodo");
		incluirNotaCabIte(contexto);
		// this.nuNota, contexto

	}

	private BigDecimal incluirNota(Map<String, Object> cabNota) throws Exception {// metodo incluirnota
		Element cabSnkElem = buildCabecalhoElem(cabNota);// ??
		System.out.println("LINHA 903");
		JapeSession.putProperty("CabecalhoNota.confirmando.nota", Boolean.FALSE);// ??
		BarramentoRegra barra = this.cacHelper.incluirAlterarCabecalho(this.sctx, cabSnkElem);// ??
		EntityPropertyDescriptor[] fds = barra.getState().getDao().getSQLProvider().getPkObjectUID()
				.getFieldDescriptors();// ??
		Collection<EntityPrimaryKey> pksEnvolvidas = barra.getDadosBarramento().getPksEnvolvidas();// ??
		EntityPrimaryKey cabKey = pksEnvolvidas.iterator().next();// ??
		for (int i = 0; i < fds.length; i++) {// for atÃ© 1 menor que seu tamanho, 1++
			EntityPropertyDescriptor cabEntity = fds[i];// ??
			if ("NUNOTA".equals(cabEntity.getField().getName())) {// se nunota for igual ??
				System.out.println("ENTROU AQUI NO IF DO FOR NUNOTA>EQUALS");
				return new BigDecimal(cabKey.getValues()[i].toString());// retorna cabkey pegando pela array
			}
		}
		System.out.println("NAO ENTROU NO IF DO FOR NUNOTA.EQUALS");
		return null;// retorna nulo se nÃ£o cair no if
	}

	private Element buildCabecalhoElem(Map<String, Object> cabecalhoNota) throws Exception {// metodo de criar cabecalho
		System.out.println("LINHA 1019");
		Element elemCabecalho = new Element("Cabecalho");// ??
		XMLUtils.addContentElement(elemCabecalho, "NUNOTA", "");// nunota sequencial?
		XMLUtils.addContentElement(elemCabecalho, "CODTIPOPER", cabecalhoNota.get("CODTIPOPER"));
		XMLUtils.addContentElement(elemCabecalho, "CODPARC", cabecalhoNota.get("CODPARC"));
		XMLUtils.addContentElement(elemCabecalho, "DTNEG", ddMMyyy.format(cabecalhoNota.get("DTNEG")));
		XMLUtils.addContentElement(elemCabecalho, "TIPMOV", cabecalhoNota.get("TIPMOV"));
		XMLUtils.addContentElement(elemCabecalho, "SERIENOTA", cabecalhoNota.get("SERIENOTA"));
		XMLUtils.addContentElement(elemCabecalho, "CODTIPVENDA", cabecalhoNota.get("CODTIPVENDA"));
		XMLUtils.addContentElement(elemCabecalho, "CODEMP", cabecalhoNota.get("CODEMP"));

		XMLUtils.addContentElement(elemCabecalho, "CIF_FOB", "F");

		return elemCabecalho;// retorna elemCabecalho
	}

	private void incluirNotaCabIte(ContextoAcao ctx) throws Exception {
		// BigDecimal nuNota, ContextoAcao ctx
		System.out.println("entrou no metodo");
		Timestamp dataHoraAtual = new Timestamp(System.currentTimeMillis());
		JdbcWrapper jdbc = JapeFactory.getEntityFacade().getJdbcWrapper();
		NativeSql nativeSql = new NativeSql(jdbc);
		SessionHandle hnd = JapeSession.open();

		for (int i = 0; i < (ctx.getLinhas()).length; i++) {
			Registro linha = ctx.getLinhas()[i];
			BigDecimal nuNotaOld = (BigDecimal) linha.getCampo("NUNOTA");

			String queryNota = (" SELECT " + "    CODCENCUS, CODTIPOPER, CODPARC " + " FROM " + "    TGFCAB "
					+ " WHERE " + "    NUNOTA = " + nuNotaOld + " AND CODEMP IN (SELECT PARCORIG FROM AD_ESPNOTADET) "
					+ " AND CODTIPOPER IN (SELECT TOPORIG FROM AD_ESPNOTADET)");

			ResultSet nota = nativeSql.executeQuery(queryNota);

			if (!nota.next()) {
				ctx.setMensagemRetorno(
						"Nota nao aprovada para gerar espelho, favor verificar as configuraçoes da tela 'Configuração Espelho da Nota'!");
				return;
			}

			try {
				System.out.println("entrou no try");
				System.out.println("pass ou da declarade query");
				JapeSession.SessionHandle hnd2 = null;

				String query2 = (" SELECT NUNOTA, CODPARC, CODEMP, CODTIPOPER, CODTIPVENDA, CODNAT, CODVEND, CODCENCUS, QTDVOL, "
						+ "VOLUME, "
						+ "CODPARCTRANSP, "
						+ "AD_OBSINT, "
						+ "OBSERVACAO, "
						+ "AD_PACSEDEX, "
						+ "TIPFRETE FROM TGFCAB "
						+ "WHERE " + "NUNOTA = " + nuNotaOld);

				System.out.println("sysout String Rs : " + query2);
				ResultSet rs = nativeSql.executeQuery(query2);
				
				while (rs.next()) {

					// codEmpNegoc = rs.getBigDecimal("CODEMPNEGOC");
					// System.out.println("tcodEmpNegoc : " + codEmpNegoc);
					// codParc = rs.getBigDecimal("CODPARC");
					// System.out.println("codParc : " + codParc);
					codNat = rs.getBigDecimal("CODNAT");
					System.out.println("codNat : " + codNat);
					// codCencus = rs.getBigDecimal("CODCENCUS");
					// System.out.println("codCencus : " + codCencus);
					topOrig = rs.getBigDecimal("CODTIPOPER");
					System.out.println("codtioper Orig : " + topOrig);
					parcOrig = rs.getBigDecimal("CODEMP");
					System.out.println("codEmp : " + parcOrig);
					nuNotaOrig = rs.getBigDecimal("NUNOTA");
					System.out.println("nunotaOrig : " + nuNotaOrig);
					codVend = rs.getBigDecimal("CODVEND");
					System.out.println("CODIGO VENDEDOR : " + codVend);
					codTipVenda = rs.getBigDecimal("CODTIPVENDA");
					System.out.println("cod tip venda : " + codTipVenda);
					System.out.println("sysout linha 605 " + codParc);
					codCencusOrig = rs.getBigDecimal("CODCENCUS");
					System.out.println("sysout centro de resultado " + codCencusOrig);
					qtdVol = rs.getBigDecimal("QTDVOL");
					System.out.println("qtdvol " + qtdVol);
					volume = rs.getString("VOLUME");
					codParcTransp = rs.getBigDecimal("CODPARCTRANSP");
					obsInt = rs.getString("AD_OBSINT");
					observacao = rs.getString("OBSERVACAO");
					pacSedex = rs.getBigDecimal("AD_PACSEDEX");
					tipFrete = rs.getString("TIPFRETE");
					
					String msg = "";

					String codVol = "";
					
						String queryEspNotDet = (" SELECT PARCORIG, " + " 	TOPORIG, " + " 	TOPDESTINO, "
								+ " 	PARCDESTINO, " + " 	PARCEIRO, " + "     CENTRORESULTADO  "
								+ " FROM AD_ESPNOTADET " + " WHERE TOPORIG = " + topOrig + " AND ATIVO = 'S'"
								+ " AND PARCORIG = " + parcOrig);// select
																	// para
																	// pegar
																	// da
																	// tela
																	// adicioal

						System.out.println("sysout String Rs : " + queryEspNotDet);
						ResultSet rse = nativeSql.executeQuery(queryEspNotDet);

						System.out.println("depois do if a top destino : " + topDestino);

						while (rse.next()) {

							topDestino = rse.getBigDecimal("TOPDESTINO");
							System.out.println("seando a top destino : " + topDestino);
							parcDestino = rse.getBigDecimal("PARCDESTINO");
							System.out.println("Empresa Destino : " + parcDestino);
							codParc = rse.getBigDecimal("PARCEIRO");
							System.out.println("Codigo Parceiro : " + codParc);
							codCencus = rse.getBigDecimal("CENTRORESULTADO");

							System.out.println("centro resultado : " + codCencus);

							System.out.println("centro resultado : " + codCencus);

							DynamicVO topVO = ComercialUtils.getTipoOperacao(this.topDestino);

							EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
							DynamicVO cabVO = (DynamicVO) dwfFacade.getDefaultValueObjectInstance("CabecalhoNota");
							cabVO.setProperty("CODTIPOPER", this.topDestino);
							System.out.println("CODTIPOPER " + topDestino);
							cabVO.setProperty("TIPMOV", topVO.asString("TIPMOV"));
							System.out.println("tipmov " + tipmov);
							cabVO.setProperty("CODPARC", this.codParc);
							System.out.println("Codparceiro " + codParc);
							cabVO.setProperty("CODEMP", this.parcDestino);
							System.out.println("parcDestino " + parcDestino);
							cabVO.setProperty("DTNEG", dataHoraAtual);
							System.out.println("DTNEG " + dataHoraAtual);
							cabVO.setProperty("CODNAT", this.codNat);
							if (codCencus == null) {
								cabVO.setProperty("CODCENCUS", this.codCencusOrig);
								System.out.println("novo codigo centro resaultado " + codCencusOrig);
							} else {
								cabVO.setProperty("CODCENCUS", this.codCencus);
								System.out.println("centro da tela  " + codCencusOrig);
							}
							cabVO.setProperty("CODVEND", this.codVend);
							System.out.println("CODVEND " + codVend);
							cabVO.setProperty("CODTIPVENDA", this.codTipVenda);
							cabVO.setProperty("SERIENOTA", "1");
							cabVO.setProperty("AD_ESPELHO", "S");
							cabVO.setProperty("OBSERVACAO", " Espelho nota, de origem nunota : " + this.nuNotaOrig);
							cabVO.setProperty("CODEMPNEGOC", this.parcDestino);
							cabVO.setProperty("DTALTER", dataHoraAtual);
							cabVO.setProperty("AD_NUNOTAESPORIG", this.nuNotaOrig);
							cabVO.setProperty("QTDVOL", this.qtdVol);
							cabVO.setProperty("VOLUME", this.volume );
							cabVO.setProperty("CODPARCTRANSP", this.codParcTransp);
							cabVO.setProperty("AD_OBSINT", this.obsInt);
							cabVO.setProperty("OBSERVACAO", this.observacao);
							cabVO.setProperty("TIPFRETE", this.tipFrete);
							cabVO.setProperty("AD_PACSEDEX", this.pacSedex);
							
							
							System.out.println("passou do cabVO");
							CACHelper cacHelper = new CACHelper();

							this.sctx = new ServiceContext(null);
							// authInfo = AuthenticationInfo.getCurrent();
							authInfo = new AuthenticationInfo("SUP", BigDecimal.ZERO, BigDecimal.ZERO,
									new Integer(2147483647));
							this.sctx.setAutentication(authInfo);
							this.sctx.putHttpSessionAttribute("usuario_logado", authInfo.getUserID());
							JapeSessionContext.putProperty("usuario_logado", authInfo.getUserID());
							this.sctx.setAttribute("usuario_logado", authInfo.getUserID());
							this.sctx.makeCurrent();

							System.out.println("passou do Helper");
							JapeSessionContext.putProperty("br.com.sankhya.com.CentralCompraVenda", Boolean.TRUE);
							System.out.println("passou do JapeSessionContext");
							PrePersistEntityState cabPreState = PrePersistEntityState.build(dwfFacade, "CabecalhoNota",
									cabVO);
							System.out.println("passou do cabPreState");
							BarramentoRegra bRegrasCab = null;
							System.out.println("auth : " + authInfo);
							System.out.println("cabPReState : " + cabPreState);
							if (authInfo != null && cabPreState != null) {
								System.out.println("entrou aqui no if da linha 658 sysout : ");
								bRegrasCab = cacHelper.incluirAlterarCabecalho(this.sctx, cabPreState);
								System.out.println("entrou aqui no if da linha 660 sysout : ");
							} else {
								System.out.println("algum dos dois está nulo.");
							}

							System.out.println("passou do bRegrascab");
							DynamicVO newCabVO = bRegrasCab.getState().getNewVO();
							System.out.println("passou do newCabVo");
							nuNota = newCabVO.asBigDecimal("NUNOTA");
							System.out.println("passou do nuNota");
							msg = "Cabecaincluido: " + nuNota;
							System.out.println("passou do msg ? " + msg);
							System.out.println(msg);
							System.out.println("novo nunota " + nuNota);

						}
					String queryEst = ("SELECT COUNT(*) AS CONTADOR, ESTOQUE FROM TGFEST " + " WHERE " + " CODPROD = "
							+ codProdIte + " AND CONTROLE = '" + controleIte + "'" + " AND CODLOCAL = "
							+ codLocalOrigIte + " AND CODEMP = " + codEmp + " GROUP BY " + " ESTOQUE ");
					System.out.println("sysout String Rs : " + queryEst);
					ResultSet rsEst = nativeSql.executeQuery(queryEst);
					System.out.println("antes do if a top destino : " + topDestino);

					if (!rsEst.next() || rsEst.getInt("CONTADOR") == 0) {
						Timestamp max = null;
						System.out.println(" Entou no if do conmtador sysout linha 146 ");
						String queryDh = ("SELECT MAX(DHALTER) AS DATA FROM TGFTOP WHERE CODTIPOPER = 3099");
						ResultSet rsDh = nativeSql.executeQuery(queryDh);
						while (rsDh.next()) {
							max = rsDh.getTimestamp("DATA");
							System.out.println("date dhalter" + max);
							JapeWrapper cabDAO = JapeFactory.dao(DynamicEntityNames.CABECALHO_NOTA);
							cabDAO.prepareToUpdateByPK(nuNota).set("CODTIPOPER", BigDecimal.valueOf(3099L)).set("DHTIPOPER", max)
									.update();
						}
					}
					this.sctx = new ServiceContext(null);
					authInfo = new AuthenticationInfo("SUP", BigDecimal.ZERO, BigDecimal.ZERO, new Integer(2147483647));
					this.sctx.setAutentication(authInfo);
					this.sctx.putHttpSessionAttribute("usuario_logado", authInfo.getUserID());
					JapeSessionContext.putProperty("usuario_logado", authInfo.getUserID());
					this.sctx.setAttribute("usuario_logado", authInfo.getUserID());
					this.sctx.makeCurrent();

					System.out.println("auth : " + authInfo);

					try {
						dwfFacade = EntityFacadeFactory.getDWFFacade();
						finde = new FinderWrapper(" ItemNota", " NUNOTA = " + nuNotaOrig);
						System.out.println("SYSOUT nunota origem incluir item  " + nuNotaOrig);
						Collection<DynamicVO> itemAtualITE = dwfFacade.findByDynamicFinderAsVO(finde);
						BigDecimal codLocal = BigDecimal.ZERO;
						for (DynamicVO item1 : itemAtualITE) {
							try {
								Collection<PrePersistEntityState> itensNota = new ArrayList<>();
								DynamicVO itemVO = (DynamicVO) dwfFacade.getDefaultValueObjectInstance("ItemNota");
								itemVO.setProperty("SEQUENCIA", item1.asBigDecimal("SEQUENCIA"));
								System.out.println("Depois do sequenc");
								itemVO.setProperty("CODPROD", item1.asBigDecimal("CODPROD"));
								System.out.println("depois do codprod");
								itemVO.setProperty("CODVOL", item1.asString("CODVOL"));
								System.out.println("depois do codvol");
								itemVO.setProperty("CODLOCALORIG", item1.asBigDecimal("CODLOCALORIG"));
								System.out.println("depois do qtdneg");
								itemVO.setProperty("QTDNEG", item1.asBigDecimal("QTDNEG"));
								System.out.println("Antes do vlrunit");
								itemVO.setProperty("VLRUNIT", item1.asBigDecimal("VLRUNIT"));
								System.out.println("Antes do c");
								// itemVO.setProperty("CONTROLE", item1.asString("CONTROLE"));
								itemVO.setProperty("CONTROLE", item1.asString("CONTROLE"));

								System.out.println("Antes do Persist");
								PrePersistEntityState itePreState = PrePersistEntityState.build(dwfFacade, "ItemNota",
										itemVO);
								System.out.println("Antes do Collection ITE");
								itensNota.add(itePreState);
								System.out.println("nunota inclusao " + nuNota);
								cacHelper.incluirAlterarItem(nuNota, authInfo, itensNota, true);

							} catch (Exception e) {
								e.printStackTrace();
								msg = "Erro na inclusao do item " + e.getMessage();
								System.out.println(msg);
								ctx.setMensagemRetorno(e.getMessage());
							}
						}
						ctx.setMensagemRetorno("Gerado lote com sucesso  = " + nuNota);
					} catch (Exception e) {
						msg = "Erro na inclusao dos Itens " + e.getMessage();
						System.out.println(msg);
						ctx.setMensagemRetorno(e.getMessage());
					}

					// JapeWrapper cabDAO = JapeFactory.dao(DynamicEntityNames.CABECALHO_NOTA);
					// cabDAO.prepareToUpdateByPK(nuNotaOrig).set("AD_ESPELHO", "S").update();
					ctx.setMensagemRetorno("Espelho nota Criado com sucesso nunota de numero  = " + nuNota);
				}
				if (nuNota == BigDecimal.ZERO) {
					ctx.setMensagemRetorno(
							"Nota não foi gerada, favor consultar log revisar lançamento, revisar restrições da TOP!");
					return;
				}
				System.out.print(this.resposta);
			} catch (Exception e) {
				System.out.println("Caiu na ultima Exception " + e.getMessage());
				e.printStackTrace();
				e.getMessage();
				ctx.setMensagemRetorno(e.getMessage());
			}
		}
	}
}
