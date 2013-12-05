Installation
############

This guide will go through the steps required to install a bleeding-edge
version of **PaPy** on a UNIX machine using the ``bash`` shell. The user is
left with two options to install **PaPy** globally or into a sandbox and to
use a stable version or possibly unstable sources.

An ``easy_install`` assumes that your default interpreter is Python 2.6, you 
have ``setuptools`` installed and you want to install **PaPy** into system-wide 
site-packages (the location where Python looks for installed libraries). If the
default Python interpreter for your operating system is different from Python 
2.6 or you do not want to put **PaPy** or its dependencies in the system 
directories or finally you'd like the latest source-code revision of **PaPy** 
read further choose the fancy way of installing **PaPy**.


Installing **PaPy** the easy way
================================ 

**PaPy** is indexed on the **PyPI** (Python Package Index). And can be installed 
simply via::

  $ su -c "easy_install papy"
    
This will install the ``papy``, ``numap``, ``nubio``, ``nubox`` and ``rpyc`` 
packages. If this did  not work please try to use the manual source code 
distributions.

  * download **PaPy**, **NuMap**, **NuBio**, **NuBox**, **RPyC**  snapshots from 
    http://muralab.org/papy
  * install it via easy_install::

      $ su -c "easy_install rpyc_YYY.tar.gz" 
      $ su -c "easy_install numap_YYY.tar.gz"    
      $ su -c "easy_install nubox_YY.tar.gz"
      $ su -c "easy_install nubio_YYY.tar.gz"    
      $ su -c "easy_install papy_YYY.tar.gz"

    replacing XXX with the version numbers of the downloaded files.

Optional dependencies to install/build/deploy **PaPy** are:

    * ``easy_install``  (``setuptools``)
    * **Sphinx**          (``sphinx``)
    
**PaPy** and Python development is much easier using the following tools:

    * ``virtualenv``
    * ``virtualevnwrapper``


Installing **PaPy** the fancy way
=================================

The fancy (and cleaner) way of using **PaPy** is to create a virtual environment
to use **PaPy**. The general stream of action is to install Python 2.6 (if 
required) install  ``setuptools`` for Python 2.6 (if required). Later we create 
sandbox environment by using ``virtualenv`` and ``virtualenvwrapper`` that is 
finally populated with **PaPy** and dependencies checked-out from a source 
repository. This guide  assumes you are using ``bash`` and a recent version of 
something UNIX-like. 


Getting Python
--------------

Most Linux distributions and Mac OSX ship a recent version of Python. You can 
(and should) skip this step if you have Python 2.6 and it is the default Python
interpreter.  To check this open a shell and type::

    $ python
    
This should return something similar to this::

    Python 2.6.2 (r262:71600, Jun 12 2009, 10:38:05)
    [GCC 4.1.2 (Gentoo 4.1.2 p1.1)] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    
Notice the version in the first line. If it is is not 2.6.x or higher, but not
3.X skip this section as you already have a supported Python version. Otherwise
it might be that your operating system still provides you a Python 2.6, but the
name of the executable is different try::
 
    $ python2.6
    
If, and only if, this fails with ``python2.6: command not found`` or 
something similar proceed with the manual system-wide installation of Python 2.6 
below.

This installation will not change the default Python interpreter for your
distribution. A compiled version of Python 2.6 for your distribution might be
available from official distribution or third-party repositories, please check 
them before instead of proceeding with the following os-independent method.
    
    #. Download Python 2.6 from http://www.python.org/download/ you want a 
       source tarball and the highest 2.6.x version available. You do not want
       to install Python 3.0.x or 3.1.x
     
    #. Unpack the compressed tarball intp any directory e.g.::
  
        tar xfv Python-2.6.2.tgz
      
    #. go to the root of the unpacked directory and compile Python2.6:

        cd 
        ./configure --prefix=/usr
        make
        su -c "make altinstall"
        
       this will install an executable ``python2.6`` into ``/usr/bin``
       
    #. try if it worked::
    
        $ python2.6
        
Be careful! Some of the following steps are different depending on whether you 
built your own Python 2.6 distribution or your system is using Python 2.6 by 
default. Make shure yout always type ``python2.6`` not ``python`` to be on the 
safe side.


Getting ``setuptools``
----------------------

We will use ``setuptools`` to install **PaPy**, the tools to necessary create a
sandbox environment and **PaPy**'s dependencies. This is a very common package
and is most likely  already installed. But we need it for explicitly for 
Python 2.6. To test if it is there it try::

    $ easy_install2.6

If you encounter an error, and you did **not** build Python 2.6 manually you 
should try to install it from your distribution's repository. On Gentoo the 
package is called "dev-python/setuptools" on Fedora it is "python-setuptools" 
and "python-setuptools-devel" for Ubuntu the package is called 
"python-setuptools" You can install them by e.g.::

    # Gentoo
    $ su -c "emerge setuptools"
    # Ubuntu
    $ sudo apt-get install python-setuptools
    # Fedora
    $ su -c "yum install python-setuptools"
    
If you had to manually compile Python 2.6 the standard distribution setuptools 
package are most likely installed but only for the default system-wide Python 
e.g.  Python 2.5. You will have to install setuptools manually for the just 
built and installed Python 2.6 interpreter.

    #. Download setuptools from http://pypi.python.org/pypi/setuptools you will 
       want the latest source version at the time of writing it is 
       setuptools-0.6c11.tar.gz.
       
    #. Unpack the compressed tarball into any directory::
    
        $ tar xvf setuptools-0.6c11.tar.gz
        
    #. Go to the root of the extracted directory::
    
        $ cd setuptools-0.6c11
    
    #. Now we install setuptools using the python 2.6 executable, but first we 
       have to make sure that we don't override ``/usr/bin/easy_install``. If 
       setuptools is by default installed for a different Python interpreter.
       If there is no other Python interpreter or you do not care you can skip
       the following and just issue:
       
        # python2.6 setup.py install
       
       To prevent overriding ``/usr/bin/easy_install`` we edit the ``setupy.py``
       file::
       
        <snip>
        "console_scripts": [
            "easy_install = setuptools.command.easy_install:main",
            "easy_install-%s = setuptools.command.easy_install:main"
            % sys.version[:3]],
        <snip>
                          
       by commenting out the second line i.e.::
       
        <snip>
        "console_scripts": [
        #   "easy_install = setuptools.command.easy_install:main",
            "easy_install-%s = setuptools.command.easy_install:main"
            % sys.version[:3]],      
        <snip>
    
    #. now we can safely run the installation::
    
        $ python2.6 setup.py install
        
    #. and verify that we have to type ``easy_install-2.6``::
        
        # CORRECT
        $ easy_install-2.6
        error: No urls, filenames, or requirements specified (see --help)
        # NOT CORRECT
        -bash: easy_install-2.6: command not found
        
        
Creating a virtual environment
------------------------------

Generally we do not want to pollute the system-wide distribution with **PaPy** 
and its dependencies, but we can and this step is optional, although maintanence
of **PaPy** might be easier in a virtual environment. We will create a virtual 
environment just for **PaPy**. We will install virtualenv and virtualenvwrapper 
into the newly created Python installation or standard Python2.6 using 
easy_install-2.6.::

    $ su -c "easy_install-2.6 virtualenv"
    $ su -c "easy_install-2.6 virtualenvwrapper"
    
Note that these packages are installed system-wide. Now we have to configure 
virtualenvwrapper on a per-user basis. We have to edit  the ``.bashrc`` file.

    #. determine where the wrapper got installed::
    
        $ which virtualenvwrapper.sh
   
    #. create a directory where you will hold the virtual enviroment(s)::
    
        $ mkdir $HOME/.virtualenvs
        
    #. add the following two lines to ``~/.bashrc`` replace __REPLACE_ME__ with 
       whatever the output from the first command was.::
       
        export WORKON_HOME=$HOME/.virtualenvs
        source __REPLACE_ME__
    
Now we have to source the edited ``.bashrc`` file::

    $ source ~/.bashrc
    
This should not generate any errors. We are finally ready to create a virtual 
Python2.6 environment for **PaPy**.::

    $ mkvirtualenv -p python2.6 --no-site-packages papy26
    
This will install a clean virtual environment called papy26 and activate it. 
Working with virtual environments is easy. To use it type ``workon papy26`` 
to leave it type ``deactivate``.


Installing **PaPy** dependencies and tools
------------------------------------------

First we have to switch to the virtual environement to host all **PaPy** related
code::
    
  $ workon papy26
    
Next we install various librarires on which **PaPy** depends. The general 
install command is::

  $ easy_install-2.6 PACKAGE_NAME

You do not have to be root to install the packages into the virtual 
environement:

  #. installing RPyC to use **PaPy** on a grid::
    
       $ easy_install-2.6 papy

  #. install Sphinx to build **PaPy** documentation::
    
       $ easy_install-2.6 sphinx

     If the above did not work because e.g. some of the tarfiles could not be 
     downloaded we have to download snapshots manually from 
     http://muralab.org/papy/downloads to install the libraries. The general 
     installation command is::
       
       $ easy_install-2.6 PACKAGE_NAME.tar.gz
       
     The required packages are ``rpyc``, ``nubox``, ``numap`` and ``nubio``.
     
                
Develop **PaPy**  
================

To install **PaPy** from sources please follow the instructions for installing
**PaPy** the fancy way and create a ``papy26_dev`` sandbox and install ``rpyc``
only (we will use ``papy``, ``nubox``, ``nubio`` and ``numap`` from the 
repository).

  #. make sure you have subversion::
     
       $ svn
       Type 'svn help' for usage. 
        
     If this returns an error you have to install the ``subversion`` package::
       
       # on Gentoo
       $ su -c "emerge subversion"    
       # on Fedora
       $ su -c "yum install subversion"
       # On Ubuntu
       $ sudo apt-get install subversion

  #. create a workspace to hold the source code
  
       $ mkdir ~/workspace
    
  #. check-out all the necessary sources:
     
       $ http://muralab.org/papy/workspace
        
  #. switch to the ``papy26_dev`` environment::
  
       $ workon papy26_dev
       
  #. add all the required source code directories::
  
       $ add2virtualenv ~/workspace/papy/src
       $ add2virtualenv ~/workspace/numap/src
       $ add2virtualenv ~/workspace/nubio/src
       $ add2virtualenv ~/workspace/nubox/src
  
  #. Verify it worked.::
    
       $ python2.6
       >>> import papy
       >>> import nubio
       >>> import nubox
       >>> import NuMap
        
