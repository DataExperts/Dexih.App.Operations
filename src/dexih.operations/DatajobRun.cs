﻿using dexih.repository;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.operations.Alerts;
using static dexih.transforms.TransformWriterResult;
using dexih.functions.Query;
using Dexih.Utils.ManagedTasks;
using Microsoft.Extensions.Logging;

namespace dexih.operations
{
	public class DatajobRun: IManagedObject
	{
		#region Events

		public delegate void DatajobProgressUpdate(TransformWriterResult writerResult);
		public event DatajobProgressUpdate OnDatajobProgressUpdate;

		public delegate void DatajobStatusUpdate(TransformWriterResult writerResult);
		public event DatajobStatusUpdate OnDatajobStatusUpdate;

		public delegate void DatalinkStart(DatalinkRun datalinkRun);
		public event DatalinkStart OnDatalinkStart;

		public delegate void Finish(DatajobRun datajobRun);
		public event Finish OnFinish;

		// private readonly BufferBlock<TransformWriterResult> _completedDatalinks = new BufferBlock<TransformWriterResult>();
		
		#endregion

		public DexihDatajob Datajob { get; set; }
		
		public List<DatalinkRun> DatalinkSteps { get; set; }
		private readonly DexihHub _hub;
		public TransformWriterResult WriterResult { get; private set; }
		private readonly Connection _auditConnection;

		private readonly TransformSettings _transformSettings;
		private readonly TransformWriterOptions _transformWriterOptions;
		private readonly ILogger _logger;
		private readonly IAlertQueue _alertQueue;
		private readonly string[] _alertEmails;
		
		private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

		public DatajobRun(TransformSettings transformSettings, ILogger logger, DexihDatajob datajob, DexihHub hub, TransformWriterOptions transformWriterOptions, IAlertQueue alertQueue, string[] alertEmails)
		{
			_transformSettings = transformSettings;
			_logger = logger;
			_alertQueue = alertQueue;
			_alertEmails = alertEmails;

			_transformWriterOptions = transformWriterOptions;

			if (datajob.AuditConnectionKey > 0)
			{
				var dbAuditConnection =
					hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == datajob.AuditConnectionKey);

				if (dbAuditConnection == null)
				{
					throw new DatalinkRunException(
						$"Audit connection with key {datajob.AuditConnectionKey} was not found.");
				}

				_auditConnection = dbAuditConnection.GetConnection(_transformSettings);
			}
			else
			{
				_auditConnection = new ConnectionMemory();
			}
			
			
			Datajob = datajob;
			_hub = hub;
		}

		public void ResetWriterResult()
		{
			WriterResult = new TransformWriterResult
			{
				AuditConnection = _auditConnection,
				AuditConnectionKey = Datajob.AuditConnectionKey ?? 0,
				AuditType = Constants.Datajob,
				HubKey = _hub.HubKey,
				ReferenceKey = Datajob.Key,
				ParentAuditKey = 0,
				ReferenceName = Datajob.Name,
				SourceTableKey = 0,
				SourceTableName = "",
				TransformWriterOptions = _transformWriterOptions,
			};
			
			WriterResult.OnProgressUpdate += Datajob_OnProgressUpdate;
			WriterResult.OnStatusUpdate += Datajob_OnStatusUpdate;
		}

		public void ResetEvents()
		{
			OnDatajobProgressUpdate = null;
			OnDatajobStatusUpdate = null;
			OnFinish = null;
		}

		public async Task Initialize(CancellationToken cancellationToken)
		{
			try
			{
				if (WriterResult == null)
				{
					ResetWriterResult();	
				}
				
				await WriterResult.Initialize(cancellationToken);
				
				DatalinkSteps = new List<DatalinkRun>();
			}
			catch (Exception ex)
			{
				WriterResult.RunStatus = ERunStatus.Abended;
				WriterResult.Message = "Error occurred initializing datajob " + Datajob.Name + ".  " + ex.Message;
				throw new DatajobRunException(WriterResult.Message, ex);
			}
		}

		public void Datajob_OnProgressUpdate(TransformWriterResult writer)
		{
			OnDatajobProgressUpdate?.Invoke(writer);
		}

		public void Datajob_OnStatusUpdate(TransformWriterResult writer)
		{
			if (_alertQueue != null && Datajob.AlertLevel != EAlertLevel.None)
            {
                switch (writer.RunStatus)
                {
                    case ERunStatus.Started:
                        if (Datajob.AlertLevel == EAlertLevel.All)
                        {
                            _alertQueue.Add(new Alert()
                            {
	                            Emails = _alertEmails,
                                Subject = $"Datajob {Datajob.Name} has started.",
                                Body = $"The datajob {Datajob.Name} started at {DateTime.Now}"
                            });
                        }

                        break;
                    case ERunStatus.Finished:
                        if (Datajob.AlertLevel == EAlertLevel.All)
                        {
                            _alertQueue.Add(new Alert()
                            {
	                            Emails = _alertEmails,
                                Subject = $"Datajob {Datajob.Name} finished successfully.",
                                Body = $"The datajob {Datajob.Name} finished successfully at {DateTime.Now}"
                            });
                        }

                        break;
                    case ERunStatus.FinishedErrors:
                        if (Datajob.AlertLevel == EAlertLevel.Errors || Datajob.AlertLevel == EAlertLevel.Critical)
                        {
                            _alertQueue.Add(new Alert()
                            {
	                            Emails = _alertEmails,
                                Subject = $"Datajob {Datajob.Name} finished with some errors.",
                                Body =
                                    $"The datajob {Datajob.Name} finished with some errors at {DateTime.Now}.\n\n{writer.Message}"
                            });
                        }

                        break;
                    case ERunStatus.Abended:
                    case ERunStatus.Cancelled:
                    case ERunStatus.Failed:
                        if (Datajob.AlertLevel != EAlertLevel.None)
                        {
                            _alertQueue.Add(new Alert()
                            {
	                            Emails = _alertEmails,
                                Subject = $"Datajob {Datajob.Name} finished with status {writer.RunStatus}.",
                                Body =
                                    $"The datajob {Datajob.Name} finished with status {writer.RunStatus} at {DateTime.Now}.\n\n{writer.Message}"
                            });
                        }

                        break;
                }
            }
			            
			OnDatajobStatusUpdate?.Invoke(writer);
		}

//		public Task Schedule(DateTime scheduledTime)
//		{
//			ResetWriterResult();	
//			WriterResult.ScheduledTime = scheduledTime;
//			return WriterResult.Schedule();
//		}

		public void CancelSchedule(CancellationToken cancellationToken)
		{
			var runStatusResult = WriterResult.SetRunStatus(ERunStatus.Cancelled, null, null);
			if (!runStatusResult)
			{
				throw new DatajobRunException($"Failed to set status");
			}

			WriterResult.ScheduledTime = null;
		}

		public async Task<bool> Run(CancellationToken cancellationToken)
		{
			try
			{
				cancellationToken.ThrowIfCancellationRequested();

				WriterResult.StartTime = DateTime.Now;
				WriterResult.LastUpdateTime = DateTime.Now;

				var runStatus = WriterResult.SetRunStatus(ERunStatus.Started, null, null);
				if (!runStatus)
				{
					throw new DatajobRunException($"Failed to set status");
				}

				if (_transformWriterOptions.TargetAction == TransformWriterOptions.ETargetAction.Truncate)
				{
					runStatus = WriterResult.SetRunStatus(ERunStatus.Started, "Truncating tables...", null);

					var targetTables = new HashSet<DexihTable>();

					foreach (var step in Datajob.DexihDatalinkSteps.Where(c => c.IsValid))
					{
						var datalink = _hub.DexihDatalinks.SingleOrDefault(c => c.IsValid && c.Key == step.DatalinkKey);

						if (datalink == null)
						{
							throw new DatajobRunException($"The datalink in the step {step.Name} with the datalink key {step.DatalinkKey} cound not be found.");
						}
						
						foreach(var target in datalink.DexihDatalinkTargets)
						{
							if(target.TableKey > 0)
							{
								var table = _hub.GetTableFromKey(target.TableKey);
								if(table != null)
								{
									targetTables.Add(table);
								}
							}
						}
					}

					// this loops through attempting to truncate tables one by one.
					// is repeats when there are failures to accomodate for some table having foriegn keys
					
					var atLeastOneSuccess = true;
					var atLeastOneFail = true;

					while (atLeastOneSuccess && atLeastOneFail)
					{
						atLeastOneSuccess = false;
						atLeastOneFail = false;

						var newTagetTables = new HashSet<DexihTable>();

						foreach (var dbTable in targetTables)
						{
							var dbConnection = _hub.DexihConnections.SingleOrDefault(c => c.Key == dbTable.ConnectionKey);

							if (dbConnection == null)
							{
								throw new DatajobRunException($"The connection for the table {dbTable.Name} with the connection key {dbTable.ConnectionKey} could not be found.");
							}
							var connection = dbConnection.GetConnection(_transformSettings);
							var table = dbTable.GetTable(_hub, connection, _transformSettings);
							try
							{
								await connection.TruncateTable(table, cancellationToken);
								atLeastOneSuccess = true;
							}
							catch
							{
								atLeastOneFail = true;
								newTagetTables.Add(dbTable);
							}
						}

						targetTables = newTagetTables;
					}

					if(targetTables.Count > 0)
					{
						var message = $"The job failed as the following tables could not be truncated: {string.Join(", ", targetTables.Select(c => c.Name))}";
						WriterResult.SetRunStatus(ERunStatus.Abended, message, null);
						return false;
					}
				}

				var inputParameters = new InputParameters();
				foreach (var parameter in Datajob.Parameters)
				{
					inputParameters.Add(new InputParameter() {Name = parameter.Name, Value =  parameter.Value, Rank = parameter.Rank});
				}

				//start all jobs async
				foreach (var step in Datajob.DexihDatalinkSteps.Where(c => c.IsValid))
				{
					var datalink = _hub.DexihDatalinks.SingleOrDefault(c => c.IsValid &&  c.Key == step.DatalinkKey);
					
					if (datalink == null)
					{
						throw new DatajobRunException($"The step {step.Name} contains a datalink with the key {step.DatalinkKey} which can not be found.");
					}

					foreach(var parameter in step.Parameters)
					{
						parameter.Value = inputParameters.SetParameters(parameter.Value, parameter.Rank);
					}

					var transformSettings = new TransformSettings
					{
						HubVariables = _transformSettings.HubVariables,
						RemoteSettings = _transformSettings.RemoteSettings,
						InputParameters = step.Parameters.ToArray<InputParameterBase>(),
						ClientFactory = _transformSettings.ClientFactory
					};
					
					var inputColumns = step.DexihDatalinkStepColumns.Select(c => c.ToInputColumn()).ToArray();
					
					var datalinkRun = new DatalinkRun(transformSettings, _logger, WriterResult.AuditKey, datalink, _hub, inputColumns, _transformWriterOptions, _alertQueue, _alertEmails) { DatalinkStepKey = step.Key};

					DatalinkSteps.Add(datalinkRun);

					//start datalinks that have no dependencies.
					if (step.DexihDatalinkDependencies == null || step.DexihDatalinkDependencies.Count == 0)
					{
						StartDatalink(datalinkRun);
					}
				}

				WriterResult.SetRunStatus(ERunStatus.Running, null, null);
				
				await WaitUntilFinished();
				
				return true;
			}
			catch (OperationCanceledException)
			{
				Cancel();
				await WaitUntilFinished();
				
				WriterResult.SetRunStatus(ERunStatus.Cancelled, "Datajob was cancelled", null);
				throw new DatajobRunException($"The datajob {Datajob.Name} was cancelled.");

			}
			catch (Exception ex)
			{
				Cancel();
				await WaitUntilFinished();
				
				var message = $"The job {Datajob.Name} failed.  {ex.Message}";
				WriterResult?.SetRunStatus(ERunStatus.Abended, message, ex);
				throw new DatajobRunException(message, ex);
			}
			finally
			{
				await WriterResult.CompleteDatabaseWrites();
				OnFinish?.Invoke(this);
			}
		}

		public void StartDatalink(DatalinkRun datalinkRun)
		{
//			datalinkRun.OnStatusUpdate += DatalinkStatus;
//			datalinkRun.OnFinish += DatalinkStatus;
						
			//raise an event to have the datalink started.
			if (OnDatalinkStart != null)
			{
				datalinkRun.ActivateDatalink();
				OnDatalinkStart.Invoke(datalinkRun);
			}
			else
			{
				throw new DatajobRunException($"Could not start the datalink {datalinkRun.Datalink.Name} due to a missing start event.");
			}

		}

		public void Schedule(DateTimeOffset startsAt, CancellationToken cancellationToken = default)
		{
			ResetWriterResult();	
			WriterResult.Schedule(startsAt, cancellationToken);
		}

		public object Data { get => WriterResult; set => throw new NotSupportedException(); }

		public async Task StartAsync(ManagedTaskProgress progress, CancellationToken cancellationToken = default)
		{
			void DatajobProgressUpdate(TransformWriterResult writerResult)
			{
				progress.Report(writerResult.PercentageComplete, writerResult.RowsTotal, writerResult.IsFinished ? "" : "Running datajob...");
			}
			
			ResetEvents();

			OnDatajobProgressUpdate += DatajobProgressUpdate;
			OnDatajobStatusUpdate += DatajobProgressUpdate;
			// OnDatalinkStart += DatalinkStart;

			progress.Report(0, 0, "Initializing datajob...");

			await Initialize(cancellationToken);

			progress.Report(0, 0, "Running datajob...");

			await Run(cancellationToken);
		}

		public void Cancel()
		{
			CancelSchedule(CancellationToken.None);
			if (DatalinkSteps == null)
			{
				return;
				
			}
			
			foreach (var step in DatalinkSteps)
			{
				step.Cancel();
			}
		}

		public void Dispose()
		{
			OnDatajobProgressUpdate = null;
			OnDatajobStatusUpdate = null;
			OnDatalinkStart = null;
			OnFinish = null;
		}
		
		private async Task WaitUntilFinished()
		{
			while (true)
			{
				var tasks = DatalinkSteps.Select(c => c.WaitForFinish()).Where(c=> c != null).ToList();
				if (tasks.Count == 0)
				{
					break;
				}

				var task = await Task.WhenAny(tasks);
				var (datalinkRun, writerResult) = await task;
				DatalinkStatus(datalinkRun, writerResult);
			}
		}

		/// <summary>
		/// Called whenever a datalink reports an update, and determines when the job should finish or start other datalinks.
		/// </summary>
		public void DatalinkStatus(DatalinkRun datalinkRun, TransformWriterResult writerResult)
		{
			if (_cancellationTokenSource.IsCancellationRequested) return;
			
			if (writerResult.RunStatus == ERunStatus.Finished || writerResult.RunStatus == ERunStatus.Abended || writerResult.RunStatus == ERunStatus.Cancelled)
			{
				WriterResult.RowsCreated += writerResult.RowsCreated;
				WriterResult.RowsUpdated += writerResult.RowsUpdated;
				WriterResult.RowsDeleted += writerResult.RowsDeleted;
				WriterResult.RowsFiltered += writerResult.RowsFiltered;
				WriterResult.RowsIgnored += writerResult.RowsIgnored;
				WriterResult.RowsPreserved += writerResult.RowsPreserved;
				WriterResult.RowsRejected += writerResult.RowsRejected;
				WriterResult.RowsReadPrimary += writerResult.RowsReadPrimary;
				WriterResult.RowsReadReference += writerResult.RowsReadReference;
				WriterResult.RowsTotal += writerResult.RowsTotal;

				WriterResult.ReadTicks += writerResult.ReadTicks;
				WriterResult.WriteTicks += writerResult.WriteTicks;
				WriterResult.ProcessingTicks = writerResult.ProcessingTicks;

				OnDatajobProgressUpdate?.Invoke(WriterResult);

				//if the datalink failed, and the job is set to abend on fail, then cancel the other datalinks.
				if (Datajob.FailAction == EFailAction.Abend && (writerResult.RunStatus == ERunStatus.Abended || writerResult.RunStatus == ERunStatus.Cancelled || writerResult.RunStatus == ERunStatus.FinishedErrors))
				{
					WriterResult.SetRunStatus(ERunStatus.Abended, "The job abended due to the datalink " + writerResult.ReferenceName + " abending.", null);
					//Cancel();
					return;
				}
				else if (writerResult.RunStatus == ERunStatus.Finished || Datajob.FailAction == EFailAction.Continue)
				{
					//see if any of the pending jobs, should be started or abended
					foreach (var datalinkStep in DatalinkSteps.Where(c => c.WriterTarget.WriterResult == null || c.WriterTarget.WriterResult.RunStatus == ERunStatus.Initialised))
					{
						var dbStep = Datajob.DexihDatalinkSteps.Single(c => c.IsValid && c.Key == datalinkStep.DatalinkStepKey);
						if (dbStep.DexihDatalinkDependencies.Any())
						{
							//check if the jobs other dependencies have finished
							var allFinished = true;
							foreach (var dbDependentStep in dbStep.DexihDatalinkDependencies)
							{
								var dependentStep = DatalinkSteps.Single(c => c.DatalinkStepKey == dbDependentStep.DependentDatalinkStepKey);

								if (dependentStep.WriterTarget.WriterResult.RunStatus != ERunStatus.Finished)
								{
									allFinished = false;
									break;
								}
							}
							if (allFinished) //all dependent jobs finished, then run the datalink.
							{
								StartDatalink(datalinkStep);
							}
							// break;
						}
					}
				}
			}

			//get an overall status
			var finished = true;
			foreach (var step in DatalinkSteps)
			{
				var result = step.WriterTarget.WriterResult;
				if (result != null) //this can sometimes be null as some datalinks have progressed whilst others are still inializing.
				{
					switch (result.RunStatus)
					{
						case ERunStatus.Cancelled:
							WriterResult.SetRunStatus(ERunStatus.Cancelled, null, null);
							break;
						case ERunStatus.Abended:
							WriterResult.SetRunStatus(ERunStatus.RunningErrors, null, null);
							break;
						case ERunStatus.Started:
						case ERunStatus.Initialised:
							finished = false;
							break;
						case ERunStatus.Running:
						case ERunStatus.RunningErrors:
							finished = false;
							break;
						case ERunStatus.Finished:
						case ERunStatus.FinishedErrors:
						case ERunStatus.NotRunning:
							break;
						default:
							throw new ArgumentOutOfRangeException();
					}
				}
			}

			if (finished)
			{
				switch (WriterResult.RunStatus)
				{
					case ERunStatus.RunningErrors:
						WriterResult.SetRunStatus(ERunStatus.FinishedErrors, null, null);
						break;
					case ERunStatus.Cancelled:
						WriterResult.SetRunStatus(ERunStatus.Cancelled, null, null);
						break;
					default:
						WriterResult.SetRunStatus(ERunStatus.Finished, null, null);
						break;
				}
			}

			OnDatajobProgressUpdate?.Invoke(WriterResult);
		}


	}
}
