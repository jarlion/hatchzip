//! The small window behind "Extract here", "Extract to a folder" and "Test".
//! The right-click was the confirmation, so it starts as soon as it shows,
//! unless the space check says no.

use std::path::{Path, PathBuf};

use anyhow::Result;
use hatchzip_core::session;
use slint::{CloseRequestResponse, ComponentHandle};

use crate::TaskWindow;
use crate::args::TaskKind;
use crate::runner::Runner;
use crate::settings::Settings;
use crate::task::Task;
use crate::work::Request;

/// Where each kind of task puts the files. `None` for a test.
pub fn output_for(kind: TaskKind, primary: &Path) -> Option<PathBuf> {
    match kind {
        TaskKind::ExtractHere => Some(primary.parent().unwrap_or(Path::new(".")).to_path_buf()),
        TaskKind::ExtractTo => Some(session::default_output(primary)),
        TaskKind::Test => None,
    }
}

pub fn run(kind: TaskKind, archive: PathBuf, no_space_check: bool) -> Result<()> {
    let (ui, start) = build(kind, archive, Settings::saved(), no_space_check)?;
    crate::backend::when_drawn(ui.window(), start);
    ui.run()?;
    Ok(())
}

/// The window, shown, and the function that starts the task. Nothing
/// touches the archive until that is called. The main window's settings
/// apply here too.
pub fn build(
    kind: TaskKind,
    archive: PathBuf,
    settings: Settings,
    no_space_check: bool,
) -> Result<(TaskWindow, impl FnOnce() + 'static)> {
    let ui = TaskWindow::new()?;
    // Only resolves the path; a missing file is reported by the run itself.
    let primary = session::locate(&archive).map_or_else(|_| archive.clone(), |(p, _)| p);
    let name = primary
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    let output = output_for(kind, &primary);
    let (task, request) = match &output {
        Some(out) => (
            Task::extract(&name, Some(out)),
            Request::Extract {
                archive,
                output: Some(out.clone()),
                keep_free: settings.keep_free,
                verify_first: settings.verify_first,
                no_space_check,
                password: None,
            },
        ),
        None => (
            Task::test(&name),
            Request::Test {
                archive,
                password: None,
            },
        ),
    };

    let weak = ui.as_weak();
    let runner = Runner::install(move |task: &Task| {
        if let Some(ui) = weak.upgrade() {
            ui.set_task(task.view.clone());
            ui.set_plan(task.plan.clone());
        }
    });
    ui.set_task(task.view.clone());

    let r = runner.clone();
    ui.on_pause(move || r.pause());
    let r = runner.clone();
    ui.on_resume(move || r.retry());
    let r = runner.clone();
    ui.on_open_folder(move || {
        let task = r.task.borrow();
        if let Some(out) = task.as_ref().and_then(|t| t.output.clone()) {
            let _ = open::that_detached(out);
        }
    });
    let r = runner.clone();
    ui.on_give_back(move || r.give_back());
    crate::password::host_in!(ui);
    let r = runner.clone();
    ui.on_dismiss(move || {
        if r.may_close() {
            let _ = slint::quit_event_loop();
        }
    });
    let r = runner.clone();
    ui.window().on_close_requested(move || {
        if r.may_close() {
            CloseRequestResponse::HideWindow
        } else {
            CloseRequestResponse::KeepWindowShown
        }
    });

    ui.show()?;
    Ok((ui, move || runner.start(task, request)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_task_writes_where_its_menu_entry_says() {
        let primary = Path::new("/home/me/Downloads/game.7z.001");
        assert_eq!(
            output_for(TaskKind::ExtractHere, primary),
            Some(PathBuf::from("/home/me/Downloads"))
        );
        assert_eq!(
            output_for(TaskKind::ExtractTo, primary),
            Some(PathBuf::from("/home/me/Downloads/game"))
        );
        assert_eq!(output_for(TaskKind::Test, primary), None);
    }
}
