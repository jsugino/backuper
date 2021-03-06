[Backuper の使い方]

java -jar jarfiile.jar DicFolder [option] [level] [src [dst]]

・DicFolder には、*.db ファイルと "folders.conf.xml" ファイルがあるフォルダを指定する。
・levelで指定されたすべてタスクのコピー作業を実行する。
・src が指定された場合、それがオリジナルとなるもののみを実行する。
・dst が指定された場合、それがコピー先となるもののみを実行する。

option	-l : ストレージやタスクを確認する。
	-f : 強制実行する。(削除や上書きするファイルが10個以上でも実行する)
	-s : スキャンのみを行う (MD5の計算も行う)
	-r : ファイル移動を反映する
	-n : 準備処理を行わない。(-sn = フルスキャン、-dn = DBを読まない)
	-d : シミュレートする。(実際のコピーは行わない。)

[組み合わせパターン]
<通常のバックアップ>
    : R -> S -> X -> W : すべて実行。(オプション -f)

<ちょっと、心配なとき>
-d  : R -> S -> P -> W : 通常スキャンしてシミュレート
-n  : R    ->   X -> W : スキャンせずに実行。(オプション -f)

<しばらくバックアップしてなかったとき>
-sn :      S    ->   W : フルスキャン
-dn : R    ->   P      : スキャンせずにシミュレート
-n  : R    ->   X -> W : スキャンせずに実行。(オプション -f)

(パラメータの例)
タスク実行：
daily
	レベル daily のタスクを実行
daily Common.C
	ストレージ Common.C をオリジナルとするタスクを実行
daily C
	ドライブ C をオリジナルとするタスクを実行
daily C D
	ドライブ C から D へコピーするタスクを実行
Common C D
	ストレージ Common.C から Common.D へコピーするタスクを実行
-d daily [src [dst]]
	レベル daily のタスクをシミュレート(コピーしない)
-n daily [src [dst]]
	スキャンををせずにレベル daily のタスクを実行(コピーする)
-dn daily [src [dst]]
	スキャンををせずにレベル daily のタスクをシミュレート(コピーしない)

確認：
-l
	すべてのタスクとストレージを表示
-l C
	Cドライブのストレージを表示
-l daily [src [dst]]
	レベル daily のタスクとストレージを表示
	(src, dst が指定されていたら、そのドライブなどに限定)

ファイル移動：
-r Common.C
	ストレージ Common.C 内でのファイル移動を *.db に反映する。
-r Common C D
	ストレージ Common.C と同等の配置となるように、Common.D 内で移動。

スキャン：
-s|-sn daily [src [dst]]
	関連するストレージの通常スキャン or フルスキャン
	(src, dst が指定されていたら、そのドライブなどに限定)
-s|-sn C
	Cドライブのすべてのストレージを通常スキャン or フルスキャン
-s|-sn Common.C
	ストレージ Common.C の通常スキャン or フルスキャン

[Backuper の使い方(旧)]

java -jar jarfiile.jar [-dsl] DicFolder [SrcName] [DstName]

DicFolder : 辞書ファイルやログファイルの格納場所
SrcName   : コピー元「識別名」を指定する。
DstName   : コピー先「識別名」を指定する。
-d        : デバッグモード。スキャンやコピーは行わず、DBの内容だけでcopy予定を表示する。
-s	  : スキップスキャン。スキャンを省略する。
-l	  : 定義ファイルの内容を出力する。

・DicFolder 以下の "folders.conf" ファイルに設定情報を記載する。
・DstName が省略されたら、SrcName に対応する辞書の更新のみを行う。

・シンボリックリンクは手繰らない。

[folders.conf.xml の記載内容]
<database>
  <folderdef id="..."> : よく使うフォルダーパターンに、id で指定した名前をつける。
     <folder dir=".../..." name="..." /> : dir(相対パス)にnameという名前をつける。
     <folder dir=".../..." name="..." />
  </folderdef>
  <storage dir="/.../..." name="..."> : dir(絶対パス)にnameという名前をつける。
    <folder dir=".../..." name="..."> : dir(相対パス)にnameという名前をつける。
      <excludes> : 無視する、ファイル or フォルダを指定する。
        ...   : スラッシュで始まらない・終わらないものは、全てのフォルダ内のファイル名
        /...  : スラッシュで始まるものは、そのフォルダからの絶対パスファイル
	/.../ : スラッシュで始まり・終わるものは、そのフォルダからの絶対パスフォルダ
      </excludes>
      <folder dir="..." name="..." /> : folder はネストして定義できる。
    </folder>
  </storage>
  <backup name="..."> : バックアップタスクを定義する。nameにはfolder名を指定する。
    <original storage="..." /> : バックアップ元の storage を指定する。
    <copy level="..." storage="..." /> : バックアップ先の storage を指定する。levelでそれを呼び出す。
    <copy level="..." storage="..." /> : バックアップ先は複数定義できる。
  </backup>
</database>

読み書きするフォルダの最小単位は Storage になる。
Storage の名前は <storage> で定義したストレージの前に、<folder> で定義したフォルダ名を連結したもの。

[<folder name="..."> の解釈ルール]
name の指定がない場合:
  ・親<folder>がない場合 dir を用いる。
  ・親<folder>がある場合は親の name に '.' で dir を連結する。
  ・dir の中の '/' は '.' に読み替えられる。
name に通常の名前を指定した場合:
  ・その名前を用いる。(親<folder>の定義は無視される。)
name に "." ではじまる名前を指定した場合: ※※※この仕様はいまいち。<backup> で ref 指定できるようにする。※※※
  ・親<folder>の name に連結する。(親がないとエラー)

[MD5を計算するタイミング]

- : ファイルが存在しない
n : ファイルがあるがMD5は計算されていない
E : ファイルがありMD5も計算ずみ
D : Delete
C : Copy
O : Copy Override

src  dst  Act  タイミング
 -    n    D   MD5の計算なしで、すぐに削除される
 -    E    D   MD5の計算なしで、すぐに削除される
 n    -    C   コピーする。コピー時にMD5を計算し、src, dst に設定される。
 E    -    C   コピーする。コピー時にMD5を計算し、src, dst に設定される。
 n    n    O   srcとdstのMD5を計算し、違っていたらコピーもする。(srcを二度読む、dstを無駄に読む)
 n    E    O   srcのMD5を計算し、違っていたらコピーもする。(srcを二度読む)
 E    n    O   dstのMD5を計算し、MD5の比較後、違っていたらコピーもする。(dstを無駄に読む)
 E    E    O   違っていたら、コピーする。
※ コピー時に計算したMD5ともとのMD5が違っていたらwarnを出す。

[folders.conf の記載内容]

<識別名1>=<フォルダ名1>
<無視するファイルパターン1>
<無視するファイルパターン2>
...
<識別名2>=<フォルダ名2>
...

<識別名> : 実行時の引数に指定する
<フォルダ名> : フォルダをフルパスで指定する
<無視するファイルパターン> : コピーしないファイルパターン
・'/' で始まるものは、パスを見てマッチする。
・'/' で始まらないものは、パスは無視する。
・'/' で終わるものは、フォルダにマッチする。
・'/' で終わらないものは、ファイルにマッチする。
・'**' は０個以上ののフォルダ・ファイルにマッチする。
・'*' は１つのフォルダ・ファイルにマッチする。

[サンプル定義ファイル]

linux.home.src=/home/junsei	# 識別名 "linux.home.src" のフォルダ名
/work/*/target/			# 無視するフォルダパス
#*#				# 無視するファイル名
*~
/.cache/
/.m2/
/.config/
/.emacs.d/semanticdb/
/.bash_history
/.local/share/gvfs-metadata/

linux.dst1=/mnt/C/BACKUP/Linux/home/junsei

linux.dst2=/mnt/D/Linux/home/junsei

[エラー対応方法]
"CANNOT COPY" と表示されるのは、コピー先に無視する対象のファイルがあり、上書きコピーできないためである。
内容を確認し、不必要なら手で削除をして、再度バックアップ処理をすること。

[*.db のフォーマット]
<fildername>	<ignorenum>
<MD5string>	<datetime>	<length>	<filename>
...

<foldername>	フォルダー名
<ignorenum>	無視したファイル・フォルダーの数
<MD5string>	MD5の計算値 (後端の '=' は取り除いてある)
<datetime>	年月日時分秒ミリ秒
<length>	ファイル長
<filename>	ファイル名

(例)
.	2
dir1	0
dcEk+ueouLiRg9TeGWYTsw	2020/10/10 10:09:49.480	3	file1-1
EfTe6lKqT1IwiZkZWQbqlw	2020/10/10 10:09:49.480	4	file1-2
r0ZBQ2Mlb/HCYsZUPQqZgQ	2020/10/10 10:09:49.480	5	file1-3
dir2	0
NH2v5b4mmi333Z4HjBOpBg	2020/10/10 10:09:49.480	6	file2-1
TuT+zXiKMLRVFBB3gBLbwg	2020/10/10 10:09:49.480	7	file2-2
wFgRwqYBVwn6/lCUAgzkIg	2020/10/10 10:09:49.480	8	file2-3

======================================================================
クラス定義

DataBase クラスは "*.db" ファイルの読み書きを行う。

	(e.g. "Linux.junsei.C")
+----------+  [storageName]  +---------+
| DataBase |---------------->| Storage | = "*.db"
+----------+		     +---------+
                               ^      ^
                    <<extens>> |      | <<extens>>
                  +--------------+  +------------+
                  | LocalStorage |  | FtpStorage |
                  +--------------+  +------------+

Storage クラスに対しする操作で、実際のファイル読み書きができる。

Backup クラスは、バックアップタスクを定義する。

		    <<List>>
+--------+ [level]  +------+ +------+ +------+
| Backup |--------> | Task |-| Task |-| Task |
+--------+          +------+ +------+ +------+
                orig |    | copy <<List>>
           +---------+    +---------+ +---------+ +---------+
           | Storage |    | Storage |-| Storage |-| Storage |
           +---------+    +---------+ +---------+ +---------+
                               | history(※)           | history(※)
                          +---------+             +---------+
                          | Storage |             | Storage |
                          +---------+             +---------+
(※) 履歴用の Storage は historyStorages.get(storageName) で取得する。
