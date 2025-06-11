# Configuration Spark sur Windows avec Hadoop (Parquet + winutils.exe + DLLs)

## Objectif

Permettre à **Apache Spark** (ex. Spark 3.^4.x) de fonctionner correctement sous **Windows**, notamment pour l’écriture de fichiers Parquet, csv etc. **sans erreurs comme :**

```
java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(...)'
```

---

##  Étapes complètes

### 1. Télécharger les binaires Hadoop 3.3.6 pour Windows

- Accédez au dépôt :
   https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin
- Téléchargez les fichiers suivants :
  - `winutils.exe`
  - `hadoop.dll`
  - `hadoop-auth.dll`

### 2. Créer un chemin d'accès (dossier)

Placez les fichiers téléchargé dans un dossier:

```
C:\hadoop\bin\
              ├── winutils.exe
              ├── hadoop.dll
              └── hadoop-auth.dll
```

### 3. Débloquer les fichiers 

Par défaut, sur  les versions récente de Windows, les fichiers téléchargé sous le format . `.dll` ou `.exe` sont automatiquement bloqué, par mesure de sécurité. Il faut donc les débloquer pour que les scripts soit de nouveau fonctionnel !!!

Pour chaque fichier dans votre dossier :
- Clic droit > **Propriétés**
- Si la case **"Débloquer"** est présente en bas → cochez-la puis cliquez sur **Appliquer**


### 4. ajouter le chemin d'accès Hadoop dans les variables d'environnement


### Ouvre les variables d’environnement :

1. Clique droit sur **Démarrer** → **Système**
2. À droite : **Paramètres système avancés**
3. Cliquer sur le bouton **Variables d’environnement**

###  Dans la section "Variables système" :

1. Trouver la ligne **Path** (⚠ dans la section *système* et non *utilisateur*)
2. Cliquer sur **Modifier...**

###  Ajouter une nouvelle entrée :

```
C:\hadoop\bin
```

###  Bien vérifier que :

- Pas de **guillemet** (`"`)
- Pas de slash (`/`) → il faut bien utiliser `\`
- Pas de caractères spéciaux comme `<`, `>`, `|`, `:`, etc.
- Pas d’espace avant ou après

 Cliquer sur **OK → OK → OK** pour tout sauvegarder


### 5.  Redémarrer l’ordinateur

Windows doit recharger le `PATH` et autoriser les DLLs déverrouillées.

