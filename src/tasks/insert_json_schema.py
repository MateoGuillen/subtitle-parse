import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import psycopg2
import json
from datetime import datetime

from config.settings import DB_CONFIG


class JSONSchemaInserter:
    def __init__(self, root):
        self.root = root
        self.root.title("Insertar JSON Schema - LLM Config")
        self.root.geometry("900x800")

        # Frame principal con scroll
        main_frame = ttk.Frame(root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configurar grid
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)

        row = 0

        # Title Slug (Identificador único)
        ttk.Label(main_frame, text="Title Slug*:", font=("Arial", 10, "bold")).grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        self.title_slug = ttk.Entry(main_frame, width=50)
        self.title_slug.grid(row=row, column=1, sticky=(tk.W, tk.E), pady=5)
        row += 1

        # Title
        ttk.Label(main_frame, text="Title*:", font=("Arial", 10, "bold")).grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        self.title = ttk.Entry(main_frame, width=50)
        self.title.grid(row=row, column=1, sticky=(tk.W, tk.E), pady=5)
        row += 1

        # Descripción
        ttk.Label(main_frame, text="Descripción:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        self.descripcion = scrolledtext.ScrolledText(main_frame, width=60, height=3)
        self.descripcion.grid(row=row, column=1, sticky=(tk.W, tk.E), pady=5)
        row += 1

        # Prompt
        ttk.Label(main_frame, text="Prompt*:", font=("Arial", 10, "bold")).grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        self.prompt = scrolledtext.ScrolledText(main_frame, width=60, height=5)
        self.prompt.grid(row=row, column=1, sticky=(tk.W, tk.E), pady=5)
        row += 1

        # JSON Schema (área principal)
        ttk.Label(main_frame, text="JSON Schema*:", font=("Arial", 10, "bold")).grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        self.json_schema = scrolledtext.ScrolledText(main_frame, width=60, height=12)
        self.json_schema.grid(row=row, column=1, sticky=(tk.W, tk.E), pady=5)
        row += 1

        # Frame para parámetros numéricos
        params_frame = ttk.LabelFrame(main_frame, text="Parámetros", padding="10")
        params_frame.grid(row=row, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=10)

        ttk.Label(params_frame, text="Temperatura:").grid(
            row=0, column=0, sticky=tk.W, padx=5
        )
        self.temperatura = ttk.Entry(params_frame, width=10)
        self.temperatura.insert(0, "0.1")
        self.temperatura.grid(row=0, column=1, sticky=tk.W, padx=5)

        ttk.Label(params_frame, text="Max Tokens:").grid(
            row=0, column=2, sticky=tk.W, padx=5
        )
        self.max_tokens = ttk.Entry(params_frame, width=10)
        self.max_tokens.insert(0, "800")
        self.max_tokens.grid(row=0, column=3, sticky=tk.W, padx=5)

        ttk.Label(params_frame, text="Modelo Recomendado:").grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.modelo_recomendado = ttk.Entry(params_frame, width=30)
        self.modelo_recomendado.grid(
            row=1, column=1, columnspan=3, sticky=(tk.W, tk.E), padx=5, pady=5
        )

        row += 1

        # Opcionales adicionales
        ttk.Label(main_frame, text="Tipo Tarea:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        self.tipo_tarea = ttk.Entry(main_frame, width=50)
        self.tipo_tarea.grid(row=row, column=1, sticky=(tk.W, tk.E), pady=5)
        row += 1

        ttk.Label(main_frame, text="Formato Salida:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        self.formato_salida = ttk.Entry(main_frame, width=50)
        self.formato_salida.grid(row=row, column=1, sticky=(tk.W, tk.E), pady=5)
        row += 1

        # Checkboxes
        checks_frame = ttk.Frame(main_frame)
        checks_frame.grid(row=row, column=0, columnspan=2, pady=10)

        self.requires_postprocessing = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            checks_frame,
            text="Requiere Postprocesamiento",
            variable=self.requires_postprocessing,
        ).pack(side=tk.LEFT, padx=10)

        self.activo = tk.BooleanVar(value=True)
        ttk.Checkbutton(checks_frame, text="Activo", variable=self.activo).pack(
            side=tk.LEFT, padx=10
        )

        row += 1

        # Botones
        buttons_frame = ttk.Frame(main_frame)
        buttons_frame.grid(row=row, column=0, columnspan=2, pady=20)

        ttk.Button(buttons_frame, text="Validar JSON", command=self.validate_json).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(buttons_frame, text="Insertar en BD", command=self.insert_data).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(buttons_frame, text="Limpiar", command=self.clear_form).pack(
            side=tk.LEFT, padx=5
        )

    def validate_json(self):
        """Valida que el JSON Schema sea válido"""
        try:
            json_text = self.json_schema.get("1.0", tk.END).strip()
            json.loads(json_text)
            messagebox.showinfo("Validación", "✅ JSON Schema válido")
        except json.JSONDecodeError as e:
            messagebox.showerror("Error", f"❌ JSON inválido:\n{str(e)}")

    def insert_data(self):
        """Inserta los datos en la base de datos"""
        # Validar campos requeridos
        if not self.title_slug.get().strip():
            messagebox.showerror("Error", "El campo 'Title Slug' es requerido")
            return

        if not self.title.get().strip():
            messagebox.showerror("Error", "El campo 'Title' es requerido")
            return

        if not self.prompt.get("1.0", tk.END).strip():
            messagebox.showerror("Error", "El campo 'Prompt' es requerido")
            return

        # Validar JSON Schema
        try:
            json_text = self.json_schema.get("1.0", tk.END).strip()
            json_obj = json.loads(json_text)
        except json.JSONDecodeError as e:
            messagebox.showerror("Error", f"JSON Schema inválido:\n{str(e)}")
            return

        # Preparar datos
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()

            insert_query = """
                INSERT INTO dncp.llm_config (
                    title_slug, title, descripcion, prompt, json_schema,
                    temperatura, max_tokens, modelo_recomendado,
                    requires_postprocessing, activo, tipo_tarea, formato_salida
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            data = (
                self.title_slug.get().strip(),
                self.title.get().strip(),
                self.descripcion.get("1.0", tk.END).strip() or None,
                self.prompt.get("1.0", tk.END).strip(),
                json.dumps(json_obj),
                float(self.temperatura.get()),
                int(self.max_tokens.get()),
                self.modelo_recomendado.get().strip() or None,
                self.requires_postprocessing.get(),
                self.activo.get(),
                self.tipo_tarea.get().strip() or None,
                self.formato_salida.get().strip() or None,
            )

            cur.execute(insert_query, data)
            conn.commit()

            messagebox.showinfo(
                "Éxito",
                f"✅ Registro insertado correctamente:\n{self.title_slug.get()}",
            )

            cur.close()
            conn.close()

            # Limpiar formulario después de insertar
            if messagebox.askyesno("Limpiar", "¿Desea limpiar el formulario?"):
                self.clear_form()

        except psycopg2.IntegrityError:
            messagebox.showerror(
                "Error",
                f"Ya existe un registro con title_slug: {self.title_slug.get()}",
            )
        except Exception as e:
            messagebox.showerror("Error", f"Error al insertar:\n{str(e)}")

    def clear_form(self):
        """Limpia todos los campos del formulario"""
        self.title_slug.delete(0, tk.END)
        self.title.delete(0, tk.END)
        self.descripcion.delete("1.0", tk.END)
        self.prompt.delete("1.0", tk.END)
        self.json_schema.delete("1.0", tk.END)
        self.temperatura.delete(0, tk.END)
        self.temperatura.insert(0, "0.1")
        self.max_tokens.delete(0, tk.END)
        self.max_tokens.insert(0, "800")
        self.modelo_recomendado.delete(0, tk.END)
        self.tipo_tarea.delete(0, tk.END)
        self.formato_salida.delete(0, tk.END)
        self.requires_postprocessing.set(False)
        self.activo.set(True)


if __name__ == "__main__":
    root = tk.Tk()
    app = JSONSchemaInserter(root)
    root.mainloop()
