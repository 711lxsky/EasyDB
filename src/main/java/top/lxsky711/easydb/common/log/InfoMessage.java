package top.lxsky711.easydb.common.log;

/**
 * @Author: 711lxsky
 * @Description:
 */

public class InfoMessage {

    public static final String RECOVER_START =
            "------------- Recover start it ----------------" +
                    "\n" +
            "-----------------------------------------------";

    public static final String PAGE_TRUNCATE =
            "Pages are truncated!";

    public static final String TRANSACTIONS_REDO_START =
            "--------- Redo transactions start. -----------" +
                    "\n" +
            "-----------------------------------------------";

    public static final String TRANSACTIONS_REDO_OVER =
            "********* Redo transactions over. ************" +
                    "\n" +
            "************************************************";

    public static final String TRANSACTIONS_UNDO_START =
            "--------- Undo transactions start. -----------" +
                    "\n" +
            "-----------------------------------------------";

    public static final String TRANSACTIONS_UNDO_OVER =
            "********* Undo transactions over. ************" +
                    "\n" +
            "************************************************";

    public static final String RECOVER_OVER =
            "********* Recover over. ************" +
                    "\n" +
            "************************************************";

    public static final String TRYING_TO_REVOKE_TRANSACTION =
            "=============== Attention ==============" +
                    "\n" +
            "==== Trying to revoke transaction...====";

    public static final String REVOKE_TRANSACTION_DONE =
            "############### Attention ##############" +
                    "\n" +
            "####### Revoke transaction done! #######";

    public static final String STATEMENT_SYNTAX_ERROR =
            "=============== Attention ===============" +
                    "\n" +
            "Your SQL statement is:  [%s] " +
                    "\n" +
            "it may has syntax error, please check near the :" +
                    "\n" +
            "                                           >> %s ";
}
