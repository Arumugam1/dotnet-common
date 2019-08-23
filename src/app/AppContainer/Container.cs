namespace automation.components.data.v1
{
    using System;
    using System.Net;
    using automation.components.container;
    using automation.components.data.v1.AppContainer;
    using automation.components.logging.v1;
    using automation.components.operations.v1;
    using Castle.MicroKernel.Registration;
    using Castle.Windsor;
    using Serilog;

    /// <summary>
    /// Provides application container singleton that is used everywhere throughout ARIC/RBA.
    ///
    /// In the future we should move away from this global singleton model and rely solely on
    /// proper dependency injection and type factories and not call (register/resolve) manually
    /// </summary>
    public static class Container
    {
        /// <summary>
        /// This is the main container object, it uses the lazy construct so that it's construction
        /// is threadsafe and to guarantee only one instance.
        ///
        /// The container is created and then invokes a windsor installer to install entities.
        /// By default that is using an entities provider that grabs the list of entities from the
        /// database.
        /// </summary>
        private static readonly Lazy<IWindsorContainer> container = new Lazy<IWindsorContainer>(() =>
        {
            LogInit.CreateLogger();
            Log.Information("Initializing Application Container");
            IWindsorContainer appContainer = new WindsorContainer();
            appContainer.Kernel.ComponentRegistered += (k, h) =>
             {
                 string msg = "Registered Component {component_key} {component_name} {@component_interfaces}";
                 Log.Information(msg, k, h.ComponentModel.Implementation.FullName, h.ComponentModel.Services);
             };
            appContainer.Install(installers);
            SetGlobalSettings();
            Log.Information("Application Container Initialization Complete");
            return appContainer;
        });

        /// <summary>
        /// The lock allows us to set the installer to be used by our container initialization
        /// and then initiailize the container atomically so that the correct installer is used
        /// in the event that multiple init methods are called from different threads.
        /// </summary>
        private static readonly object padlock = new object();

        /// <summary>
        /// Installer to use in container initialization.  Once container init is finished
        /// this value no longer matters.
        /// </summary>
        private static IWindsorInstaller[] installers;

        /// <summary>
        /// Initialize a container.  This method forces initialization of the 'isInitialized' variable.
        ///
        /// Once this is done, future calls to the 'Resolve' method will actually instantiate the container
        /// object versus just returning null.  This hoop jumping is to conform to the existing "Contract"
        /// of this class.  Currently the behavior is calling 'Resolve' will return null until Init() is called.
        /// It is important to hold to that behavior because 'Resolve' is called EVERYWHERE including in some
        /// places before/during test setups that are expecting to create a mock container, not a real one.
        /// If calls to 'Resolve' before Init() actually initialize a real container pretty much all tests
        /// that rely on a mocked container will fail.
        ///
        /// The reason QuestionableInit is called here and not from within the lazy value is that it uses a class
        /// that calls 'Resolve' and so creates a circular dependency/deadlock on the lazy value.
        /// </summary>
        public static void Init()
        {
            if (!container.IsValueCreated)
            {
                Init(
                    Installer.WithInterfaces(typeof(ICoreComponents), typeof(IEdgeComponents)),
                    new LegacyInstaller(new DbConfigEntitiesProvider())
                );

                QuestionableInit();
            }
        }

        public static string Init(params IWindsorInstaller[] windsorInstallers)
        {
            try
            {
                lock (padlock)
                {
                    installers = windsorInstallers;
                    return container.Value.Name;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error initializing the application container");
                throw ex;
            }
        }

        /// <summary>
        /// Similar to Init() this call toggles the isDumbInitialized such that calls to ResolveCustom will
        /// initialize the lazy container and return real values as opposed to null to maintain the current
        /// contract.
        /// </summary>
        public static void InitializeConfig()
        {
            Init(new LegacyInstaller(new XmlConfigEntitiesProvider()));
        }

        public static void Register<T, TAs>()
            where T : TAs
        {
            Init();

            // OnlyNewServices check is not atomic with registration which can lead to duplicate
            // registration exceptions, this lock prevents these exceptions.
            lock (padlock)
            {
                container.Value.Register(Component.For(typeof(TAs)).ImplementedBy(typeof(T)).OnlyNewServices());
            }
        }

        public static void Register<T, TAs>(T instance)
            where T : TAs
        {
            Init();

            // OnlyNewServices check is not atomic with registration which can lead to duplicate
            // registration exceptions, this lock prevents these exceptions.
            lock (padlock)
            {
                container.Value.Register(Component.For(typeof(TAs)).Instance(instance).OnlyNewServices());
            }
        }

        public static void RegisterAs<TAs>(TAs instance)
        {
            Init();

            // OnlyNewServices check is not atomic with registration which can lead to duplicate
            // registration exceptions, this lock prevents these exceptions.
            lock (padlock)
            {
                container.Value.Register(Component.For(typeof(TAs)).Instance(instance).OnlyNewServices());
            }
        }

        /// <summary>
        /// Initializes the container in a mock (empty) state just as LocalContainer.Init() does
        /// then registers the instance that is passed in as the component.
        /// Unlike the other "real" register methods, this one will override prior implmentations.
        /// Because this doesn't clean up those prior implementations (as that appears to be impossible
        /// in Castle Windsor without calling Dispose() on the whole container).  Use of this method
        /// can lead to memory leaks and SHOULD NEVER be called in production.  This is being created
        /// this way only to support backwards compatibility of existing usages in tests.  In the
        /// future the container will not be a static global singleton and so to achieve this, you would just create a new
        /// container in the tests that need new components.
        /// </summary>
        /// <typeparam name="TAs"></typeparam>
        /// <param name="instance"></param>
        public static void RegisterAsForMock<TAs>(TAs instance)
        {
            Init(new LegacyInstaller());
            container.Value.Register(Component.For(typeof(TAs)).Instance(instance).IsDefault().Named(Guid.NewGuid().ToString()));
        }

        public static T Resolve<T>()
        {
            if (container.IsValueCreated && container.Value.Kernel.HasComponent(typeof(T)))
            {
                return container.Value.Resolve<T>();
            }
            else
            {
                return default(T);
            }
        }

        /// <summary>
        /// As far as I can tell, these reload methods are only used by queuebased
        /// https://github.rackspace.com/automation/dotnet.service.queuebased/blob/461208c271bdf7b48474d54290d5695dd063fda8/src/app/Main.cs#L491
        /// I've confirmed with Vijay that this reload option to queuebased is not longer used
        /// as part of regular operations to this service, and I plan to submit a PR to remove it.
        /// Even in it's current form, I'm pretty sure reloading would cause memory leaks as nothing
        /// is disposed of before things are loaded again.  The new model has no support for reloading
        /// once initialized because of the nature of the Lazy value.
        /// </summary>
        [Obsolete("Reloading is not supported, stop the application and restart")]
        public static void Reload()
        {
            Init();
        }

        [Obsolete("We have condensed down to one container, use plain Regiter methods. " +
                  "This is for backwards compatibility only")]
        public static void RegisterCustom<T, TAs>()
            where T : TAs
        {
            InitializeConfig();
            Register<T, TAs>();
        }

        [Obsolete("We have condensed down to one container, use plain Regiter methods. " +
                  "This is for backwards compatibility only")]
        public static void RegisterCustom<T, TAs>(T instance)
            where T : TAs
        {
            InitializeConfig();
            Register<T, TAs>(instance);
        }

        [Obsolete("We have condensed down to one container, use plain Regiter methods. " +
                  "This is for backwards compatibility only")]
        public static void RegisterAsCustom<TAs>(TAs instance)
        {
            InitializeConfig();
            RegisterAs<TAs>(instance);
        }

        [Obsolete("We have condensed down to one container, use plain Resolve method. " +
                  "This is for backwards compatibility only")]
        public static T ResolveCustom<T>()
        {
            return Resolve<T>();
        }

        /// <summary>
        /// As far as I can tell, these reload methods are only used by queuebased
        /// https://github.rackspace.com/automation/dotnet.service.queuebased/blob/461208c271bdf7b48474d54290d5695dd063fda8/src/app/Main.cs#L491
        /// I've confirmed with Vijay that this reload option to queuebased is not longer used
        /// as part of regular operations to this service, and I plan to submit a PR to remove it.
        /// Even in it's current form, I'm pretty sure reloading would cause memory leaks as nothing
        /// is disposed of before things are loaded again.  The new model has no support for reloading
        /// once initialized because of the nature of the Lazy value.
        /// </summary>
        [Obsolete("Reloading is not supported, stop the application and restart")]
        public static void ReloadConfig()
        {
            InitializeConfig();
        }

        private static void SetGlobalSettings()
        {
            // DefaultConnectionLimit from application
            ServicePointManager.DefaultConnectionLimit = 100000;

            // Accepts All Certificates will have to be removed once we have Certificates Upload feature - This Feature is removed
            ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, errors) => true;

            // Adding to fix https://jira.rax.io/browse/ARIC-11936 since Communicator constructors are no longer
            // called eagerly.
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;

            // Removing configurability of this as it is set this way in every environment. Can be added back as AppSetting if needed.
            System.Runtime.GCSettings.LatencyMode = System.Runtime.GCLatencyMode.SustainedLowLatency;
        }

        /// <summary>
        /// The reason this init is questionable is because:
        ///
        /// - Fluent cassandra has stopped being used, but there are still calls to generate a timebased guid in automation
        /// projects.  It's unclear what taking this config out would do to those usages, but removing thse guid calls and
        /// this config should be done
        ///
        /// - Email settings have no place here.  They should be initialized in the object in question when it is used.  Likely
        /// from within a constructor.
        /// </summary>
        private static void QuestionableInit()
        {
            var timeServer = Config.Manager.GetApplicationConfigValue("TimeServer", "AllApplications");
            FluentCassandra.TimestampHelper.UtcNow = () => DateTimeExtension.GetTimeServerUTC(timeServer);

            Email.SetConfig(
                Config.Manager.GetApplicationConfigValue("ValidAddresses", "AllApplications.Email"),
                Config.Manager.GetApplicationConfigValue("DefaultToAddress", "AllApplications.Email"),
                Config.Manager.GetApplicationConfigValue("DefaultSMTPServer", "AllApplications.Email"),
                Config.Manager.GetApplicationConfigValue("DefaultFooter", "AllApplications.Email"));
        }
    }
}