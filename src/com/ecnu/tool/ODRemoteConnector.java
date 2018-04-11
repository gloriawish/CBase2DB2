package com.ecnu.tool;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ODRemoteConnector {
	private Connection conn = null;
	private boolean isAuthenticated;
	private ExecutorService executor = Executors.newSingleThreadExecutor();

	private static boolean PRINT_SHELL = false;

	private static String[] IGNORED_PREFIX = { "kill: usage", "cat:" };

	public ODRemoteConnector(String ip, String username, String password) {
		try {
			this.conn = new Connection(ip);
			this.conn.connect();
			this.isAuthenticated = this.conn.authenticateWithPassword(username, password);
			if (!this.isAuthenticated)
				LoggerTool.error("[ERROR] Connect to [" + ip + "] using [user:'" + username + "', password:'" + password
						+ "'] fail!", new Throwable().getStackTrace());
		} catch (Exception e) {
			LoggerTool.error("[ERROR] Connect to " + ip + " timeout!", new Throwable().getStackTrace());
			e.printStackTrace();
		}
	}

	public boolean isConnectSuccess() {
		return this.isAuthenticated;
	}

	public void executeDirect(String cmd) {
		execute(cmd, false, false, false);
	}

	public Pair<ODError, String> executeValue(String cmd) {
		return execute(cmd, true, false, false);
	}

	public Pair<ODError, String> executeWaiting(String cmd) {
		return execute(cmd, true);
	}

	public Pair<ODError, String> execute(String cmd) {
		return execute(cmd, false);
	}

	public Pair<ODError, String> execute(String cmd, boolean needCount) {
		return execute(cmd, true, needCount, true);
	}

	private Pair<ODError, String> execute(String cmd, boolean needResult, boolean needCount, boolean needLineNumber) {
		ODError ret = ODError.SUCCESS;
		String result = null;
		ODCountRunnable countRunnable = null;
		try {
			if (this.isAuthenticated) {
				Session session = this.conn.openSession();
				if (needCount) {
					countRunnable = new ODCountRunnable();
					this.executor.submit(countRunnable);
				}
				session.execCommand(cmd);
				if (needResult) {
					String err = readInputStream(session.getStderr(), needLineNumber);
					String out = readInputStream(session.getStdout(), needLineNumber);
					if ((err != null) && (err.length() > 0)) {
						if (PRINT_SHELL) {
							System.out.println("ERROR:");
						}
						ret = ODError.ERROR;
						result = err;
					} else if ((out != null) && (out.length() > 0)) {
						if (PRINT_SHELL) {
							System.out.println("SUCCESS:");
						}
						result = out;
					}
					if (PRINT_SHELL) {
						System.out.println(result);
						System.out.println("------------ End shell -----------");
					}
				}

				session.close();
			} else {
				ret = ODError.ERROR;
			}
		} catch (Exception e) {
			e.printStackTrace();
			ret = ODError.ERROR;
		} finally {
			if (countRunnable != null) {
				countRunnable.stop();
			}
		}
		return new Pair(ret, result);
	}

	private String readInputStream(InputStream in, boolean needLineNumber) throws IOException {
		StringBuilder retBuffer = new StringBuilder();
		BufferedReader bReader = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset().toString()));

		int index = 1;
		String line = null;
		while ((line = bReader.readLine()) != null) {
			boolean isIgnored = false;
			for (String prefix : IGNORED_PREFIX) {
				if (line.startsWith(prefix)) {
					isIgnored = true;
					break;
				}
			}
			if (!isIgnored) {
				if (needLineNumber) {
					String indexStr = index + "  ";
					retBuffer.append(indexStr.substring(0, 2)).append(":");
				}
				retBuffer.append(line).append(System.getProperty("line.separator"));
				index++;
			}
		}
		bReader.close();
		String ret = retBuffer.toString();
		if (ret.length() > System.getProperty("line.separator").length()) {
			ret = ret.substring(0, ret.length() - System.getProperty("line.separator").length());
		}
		return ret;
	}

	public void close() {
		try {
			if (this.conn != null) {
				this.conn.close();
			}
			if (this.executor != null)
				this.executor.shutdownNow();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private class ODCountRunnable implements Runnable {
		private boolean isStop = false;

		private ODCountRunnable() {
		}

		public void run() {
			int i = 1;
			System.out.print("Wait: ");
			while (!this.isStop) {
				System.out.print(i);
				if (!this.isStop) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					for (int j = 0; j < String.valueOf(i).length(); j++) {
						System.out.print("\b");
					}
					i++;
				}
			}
			System.out.println(i - 1);
		}

		void stop() {
			this.isStop = true;
		}
	}
}
